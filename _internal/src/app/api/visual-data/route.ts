import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

// Serve visual.json from the latest forecast context directory
const CONTEXTS_DIR = path.resolve(
  process.cwd(),
  "..",
  "data",
  "contexts",
);

function findLatestFile(filename: string): string | null {
  if (!fs.existsSync(CONTEXTS_DIR)) return null;

  const dirs = fs
    .readdirSync(CONTEXTS_DIR)
    .filter((d) => d.includes("forecast_"))
    .sort()
    .reverse();

  for (const dir of dirs) {
    const filePath = path.join(CONTEXTS_DIR, dir, filename);
    if (fs.existsSync(filePath)) return filePath;
  }
  return null;
}

export async function GET() {
  const jsonPath = findLatestFile("visual.json");

  if (!jsonPath) {
    return NextResponse.json(
      { error: "No visual.json found. Run the forecast_setup notebook first." },
      { status: 404 },
    );
  }

  const content = fs.readFileSync(jsonPath, "utf-8");
  return new NextResponse(content, {
    headers: { "Content-Type": "application/json" },
  });
}
