#!/usr/bin/env -S deno run --allow-all
// End-to-end: a backend (examples/test_backend.rs: no HTTP, its zenoh dials out to the relay) with 2 cameras and
// data topics, the relay, then 1 and 3 headless Chrome viewers. Checks every viewer gets video and data, a viewer's
// put reaches the backend, the backend pulls each camera once and encodes it once at 1 and at 3 viewers with its CPU
// roughly flat, the relay refuses a bad token, listTopics shows the backend's topics, and upstream closes after the
// last viewer.
// Usage: deno run --allow-all test/e2e.js   (E2E_VERBOSE=1 prints the processes' output)

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { launch } from "jsr:@astral/astral@0.5.6"

const repoRoot = $.path(import.meta.url).parentOrThrow().parentOrThrow()
const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-relay-e2e-" }))
const measureSeconds = Number(Deno.env.get("MEASURE_SECONDS") ?? 10)

/** @type {string[]} */
const failures = []
/** @param {boolean} condition @param {string} description */
function check(condition, description) {
    console.log(`${condition ? "PASS" : "FAIL"} ${description}`)
    if (!condition) {
        failures.push(description)
    }
}

function freePort() {
    const listener = Deno.listen({ port: 0, hostname: "127.0.0.1" })
    const port = /** @type {Deno.NetAddr} */ (listener.addr).port
    listener.close()
    return port
}

/** A child's output lines, with waiters. @param {ReadableStream<Uint8Array>} stream @param {string} name */
function lines(stream, name) {
    /** @type {string[]} */
    const all = []
    /** @type {{ test: (line: string) => boolean, resolve: (line: string) => void }[]} */
    let waiters = []
    ;(async () => {
        let buffered = ""
        for await (const chunk of stream.pipeThrough(new TextDecoderStream())) {
            buffered += chunk
            const parts = buffered.split("\n")
            buffered = parts.pop() ?? ""
            for (const line of parts) {
                all.push(line)
                if (Deno.env.get("E2E_VERBOSE")) {
                    console.log(`[${name}] ${line}`)
                }
                waiters = waiters.filter((waiter) => !(waiter.test(line) && (waiter.resolve(line), true)))
            }
        }
    })()
    return {
        all,
        /** @param {(line: string) => boolean} test @param {number} timeoutMs @returns {Promise<string>} */
        waitFor(test, timeoutMs) {
            const existing = all.find(test)
            if (existing) {
                return Promise.resolve(existing)
            }
            return new Promise((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error(`${name}: timed out waiting for a line`)), timeoutMs)
                waiters.push({ test, resolve: (line) => (clearTimeout(timer), resolve(line)) })
            })
        },
    }
}

/** CPU seconds a process has used (ps `time`: [[dd-]hh:]mm:ss.cc). @param {number} pid */
async function cpuSeconds(pid) {
    const text = (await $`ps -o time= -p ${pid}`.text()).trim()
    return text.split(/[-:]/).map(Number).reduce((total, part) => total * 60 + part, 0)
}

/** @type {Deno.ChildProcess[]} */
const children = []
/** @type {import("jsr:@astral/astral@0.5.6").Browser | undefined} */
let browser

try {
    $.logStep("building the relay and the test backend (release)")
    await $`cargo build --release --bin zenoh-web-relay --example test_backend`.cwd(repoRoot)
    $.logStep("bundling zenoh-web's browser client")
    const metadata = JSON.parse(await $`cargo metadata --format-version 1`.cwd(repoRoot).text())
    const zenohWebRoot = $.path(metadata.packages.find((/** @type {{ name: string }} */ crate) => crate.name === "zenoh-web").manifest_path).parentOrThrow().parentOrThrow()
    const web = scratch.join("web")
    web.join("client").mkdirSync({ recursive: true })
    web.join("index.html").writeTextSync("<!doctype html><title>viewer</title><body></body>")
    const bundled = await new Deno.Command(Deno.execPath(), { args: ["bundle", "--quiet", "--platform", "browser", "-o", web.join("client/zenoh_web.js").toString(), zenohWebRoot.join("client/zenoh_web.ts").toString()] }).output()
    if (bundled.code !== 0) {
        throw new Error(`deno bundle failed: ${new TextDecoder().decode(bundled.stderr)}`)
    }

    const authFile = scratch.join("tokens.json5")
    authFile.writeTextSync(JSON.stringify({ tokens: { viewer: "write" } }))
    const [zenohPort, httpPort] = [freePort(), freePort()]
    const relayUrl = `http://127.0.0.1:${httpPort}`
    const relayCommand = new Deno.Command(repoRoot.join("target/release/zenoh-web-relay").toString(), {
        args: ["--listen", `tcp/127.0.0.1:${zenohPort}`, "--http", `127.0.0.1:${httpPort}`, "--backend-name", "robot", "--backend-token", "relay-secret", "--auth-file", authFile.toString(), "--serve", web.toString()],
        env: { RUST_LOG: Deno.env.get("RUST_LOG") || "info,zenoh=warn,zenoh_ext=warn,zenoh_web=info,zenoh_web_relay=info,rtc=warn,webrtc=warn" },
        stdout: "inherit",
        stderr: "piped",
    })
    const relay = relayCommand.spawn()
    children.push(relay)
    const relayOutput = lines(relay.stderr, "relay")
    await relayOutput.waitFor((line) => line.includes("waiting for backend"), 15000)

    const backend = new Deno.Command(repoRoot.join("target/release/examples/test_backend").toString(), {
        args: ["--connect", `tcp/127.0.0.1:${zenohPort}`, "--name", "robot", "--token", "relay-secret", "--cameras", "2", "--size", "640x480", "--fps", "30"],
        stdout: "piped",
        stderr: "inherit",
    }).spawn()
    children.push(backend)
    const backendOutput = lines(backend.stdout, "backend")
    await backendOutput.waitFor((line) => line === "READY", 15000)
    await relayOutput.waitFor((line) => line.includes("listening on"), 20000)
    console.log(`relay ${relayOutput.all.find((line) => line.includes("video encoder:"))?.replace(/.*\] /, "") ?? "video encoder: ?"}`)
    check(true, "the backend dialled out and the relay connected to it over zenoh signalling (the backend has no HTTP listener)")
    /** @returns {{ subscriptions: [string, string | null][], encoders: number, encodedFrames: number }} */
    const backendStats = () => JSON.parse(backendOutput.all.findLast((line) => line.startsWith("STATS "))?.slice(6) ?? "{}")

    browser = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })

    /** Opens a viewer page that subscribes to both cameras and the data topics and puts once. @param {number} index */
    async function openViewer(index) {
        const page = await browser.newPage(`${relayUrl}/index.html`)
        const outcome = await page.evaluate(async ([relayUrl, index]) => {
            const { connect } = await import("/client/zenoh_web.js")
            const z = await connect(relayUrl, { token: "viewer", heartbeatHz: 5 })
            const counts = { cam0: 0, cam1: 0, cam0Width: 0, counter: 0, lastCounter: "", depth: 0, depthSize: 0 }
            globalThis.counts = counts
            globalThis.videos = []
            for (const camera of [0, 1]) {
                const subscription = z.subscribe(`cam/${camera}`, { encoding: "test-pattern" }, (msg) => {
                    if (msg.video) {
                        counts[`cam${camera}`]++
                        if (camera === 0) {
                            counts.cam0Width = msg.video.width
                        }
                    }
                })
                await subscription.ready()
                const video = document.createElement("video")
                Object.assign(video, { muted: true, autoplay: true, playsInline: true, srcObject: subscription.mediaStream })
                document.body.append(video)
                globalThis.videos.push(video)
                video.play().catch(() => {})
            }
            await z.subscribe("data/counter", {}, (msg) => {
                counts.counter++
                counts.lastCounter = new TextDecoder().decode(msg.bytes)
            }).ready()
            await z.subscribe("data/depth", { encoding: "test-fields" }, (msg) => {
                counts.depth++
                counts.depthSize = msg.decoded?.size ?? -1
            }).ready()
            const publisher = z.publisher(`cmd/viewer${index}`, { delivery: "reliable" })
            await publisher.ready()
            publisher.put(`hello from viewer ${index}`)
            // the relay refreshes the backend's topics every 3 s
            let topics = []
            for (let tries = 0; tries < 20 && !topics.includes("cam/0"); tries++) {
                topics = (await z.listTopics("**", { probeMs: 0 })).map((topic) => topic.key)
                await new Promise((resolve) => setTimeout(resolve, 250))
            }
            globalThis.z = z
            return { topics }
        }, { args: [[relayUrl, index]] })
        return { page, ...outcome }
    }

    /** Each viewer's counts and decoded video frames. @param {{ page: any }[]} viewers */
    const sample = (viewers) => Promise.all(viewers.map(({ page }) => page.evaluate(() => ({
        ...globalThis.counts,
        decodedFrames: globalThis.videos.map((video) => video.getVideoPlaybackQuality().totalVideoFrames),
    }))))

    /** Backend CPU, encodes and viewers' traffic over `measureSeconds`. @param {{ page: any }[]} viewers */
    async function measure(viewers) {
        const [cpu0, relayCpu0, stats0, counts0] = [await cpuSeconds(backend.pid), await cpuSeconds(relay.pid), backendStats(), await sample(viewers)]
        await $.sleep(measureSeconds * 1000)
        const [cpu1, relayCpu1, stats1, counts1] = [await cpuSeconds(backend.pid), await cpuSeconds(relay.pid), backendStats(), await sample(viewers)]
        const perViewer = counts1.map((after, index) => ({
            cam0Fps: (after.cam0 - counts0[index].cam0) / measureSeconds,
            cam1Fps: (after.cam1 - counts0[index].cam1) / measureSeconds,
            decodedFps: after.decodedFrames.map((frames, video) => (frames - counts0[index].decodedFrames[video]) / measureSeconds),
            counterHz: (after.counter - counts0[index].counter) / measureSeconds,
            depthHz: (after.depth - counts0[index].depth) / measureSeconds,
            depthSize: after.depthSize,
            lastCounter: after.lastCounter,
            cam0Width: after.cam0Width,
        }))
        return {
            viewers: viewers.length,
            backendCpuPercent: Math.round((cpu1 - cpu0) / measureSeconds * 1000) / 10,
            relayCpuPercent: Math.round((relayCpu1 - relayCpu0) / measureSeconds * 1000) / 10,
            backendSubscriptions: stats1.subscriptions,
            backendEncoders: stats1.encoders,
            backendEncodedFps: (stats1.encodedFrames - stats0.encodedFrames) / measureSeconds,
            perViewer,
        }
    }

    /** @param {Awaited<ReturnType<typeof measure>>} result */
    function checkPhase(result) {
        const label = `${result.viewers} viewer(s)`
        result.perViewer.forEach((viewer, index) => {
            check(viewer.cam0Fps > 15 && viewer.cam1Fps > 15, `${label}: viewer ${index} gets both cameras (${viewer.cam0Fps} / ${viewer.cam1Fps} fps)`)
            check(viewer.decodedFps.every((fps) => fps > 15), `${label}: viewer ${index}'s <video> elements decode them (${viewer.decodedFps} fps)`)
            check(viewer.counterHz > 7 && viewer.lastCounter.startsWith("count "), `${label}: viewer ${index} gets the raw data topic (${viewer.counterHz} Hz, "${viewer.lastCounter}")`)
            check(viewer.depthHz > 7 && viewer.depthSize === 65536, `${label}: viewer ${index} gets the fields topic, passed through (${viewer.depthHz} Hz, size ${viewer.depthSize})`)
        })
        for (const camera of ["cam/0", "cam/1"]) {
            const subscriptions = result.backendSubscriptions.filter(([key]) => key === camera)
            check(subscriptions.length === 1, `${label}: the backend has exactly 1 subscription on ${camera} (${JSON.stringify(subscriptions)})`)
        }
        check(result.backendSubscriptions.length === 4, `${label}: the backend has 4 subscriptions in all, 2 cameras + 2 data topics (${JSON.stringify(result.backendSubscriptions)})`)
        check(result.backendEncoders === 2, `${label}: the backend runs exactly 1 encoder per camera (${result.backendEncoders} for 2 cameras)`)
        check(result.backendEncodedFps > 45 && result.backendEncodedFps < 70, `${label}: the backend encodes each frame once, ~60 frames/s for 2 cameras at 30 fps (${result.backendEncodedFps})`)
    }

    $.logStep("1 viewer")
    const viewers = [await openViewer(0)]
    check(["cam/0", "cam/1", "data/counter", "data/depth"].every((key) => viewers[0].topics.includes(key)), `listTopics on the relay shows the backend's topics (${viewers[0].topics})`)
    await $.sleep(3000)
    const one = await measure(viewers)
    console.log(JSON.stringify(one))
    checkPhase(one)

    $.logStep("3 viewers")
    viewers.push(await openViewer(1), await openViewer(2))
    await $.sleep(3000)
    const three = await measure(viewers)
    console.log(JSON.stringify(three))
    checkPhase(three)
    const ratio = three.backendCpuPercent / one.backendCpuPercent
    check(ratio < 1.25, `backend CPU stays roughly flat from 1 to 3 viewers: ${one.backendCpuPercent}% -> ${three.backendCpuPercent}% (x${ratio.toFixed(2)}); relay ${one.relayCpuPercent}% -> ${three.relayCpuPercent}%`)

    for (const index of [0, 1, 2]) {
        await backendOutput.waitFor((line) => line === `RECV cmd/viewer${index} hello from viewer ${index}`, 5000).catch(() => {})
        check(backendOutput.all.includes(`RECV cmd/viewer${index} hello from viewer ${index}`), `viewer ${index}'s put reached the backend`)
    }

    $.logStep("auth")
    const refused = await viewers[0].page.evaluate(async (relayUrl) => {
        const { connect } = await import("/client/zenoh_web.js")
        const outcome = (promise) => promise.then(() => "accepted", (error) => error.message)
        return { bad: await outcome(connect(relayUrl, { token: "nope", reconnect: false })), none: await outcome(connect(relayUrl, { reconnect: false })) }
    }, { args: [relayUrl] })
    check(refused.bad.includes("unknown token"), `the relay refuses a bad token (${refused.bad})`)
    check(refused.none.includes("a token is required"), `the relay refuses a missing token (${refused.none})`)

    $.logStep("last viewer leaves")
    for (const { page } of viewers) {
        await page.evaluate(() => globalThis.z.close())
        await page.close()
    }
    let remaining = backendStats().subscriptions
    for (let waited = 0; waited < 20000 && remaining.length > 0; waited += 500) {
        await $.sleep(500)
        remaining = backendStats().subscriptions
    }
    check(remaining.length === 0, `upstream subscriptions close after the last viewer (${JSON.stringify(remaining)})`)

    scratch.join("results.json").writeTextSync(JSON.stringify({ one, three }, null, 4))
} catch (error) {
    console.error(error)
    failures.push(`error: ${error}`)
} finally {
    await browser?.close().catch(() => {})
    for (const child of children) {
        try {
            child.kill("SIGTERM")
        } catch {
            // already exited
        }
    }
}
console.log(`\n${failures.length === 0 ? "ALL PASSED" : `${failures.length} FAILED:\n  ${failures.join("\n  ")}`}`)
console.log(`artifacts: ${scratch}`)
Deno.exit(failures.length === 0 ? 0 : 1)
