import { spawn } from "node:child_process"

const [gate, mode] = process.argv.slice(2)
if (gate === undefined) throw new Error("Missing release gate")

if (mode === "descendant") {
  await Bun.write(gate + ".ready", "")
  while (!(await Bun.file(gate).exists())) await Bun.sleep(5)
  process.exit(0)
}

const descendant = spawn(process.execPath, [import.meta.filename, gate, "descendant"], {
  detached: true,
  stdio: ["ignore", "ignore", 2],
})
descendant.unref()
while (!(await Bun.file(gate + ".ready").exists())) await Bun.sleep(5)
process.exit(0)
