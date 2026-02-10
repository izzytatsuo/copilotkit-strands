import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

export const dynamic = "force-dynamic";

// Serve joined.csv from the latest forecast context directory
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

function parseCsvToJson(csv: string): Record<string, string | null>[] {
  const lines = csv.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]);
  return lines.slice(1).map((line) => {
    const values = parseCsvLine(line);
    const obj: Record<string, string | null> = {};
    headers.forEach((h, i) => {
      const v = values[i] ?? "";
      obj[h] = v === "" ? null : v;
    });
    return obj;
  });
}

// Handle quoted CSV fields (strips surrounding quotes, handles escaped quotes)
function parseCsvLine(line: string): string[] {
  const fields: string[] = [];
  let i = 0;
  while (i < line.length) {
    if (line[i] === '"') {
      let j = i + 1;
      let value = "";
      while (j < line.length) {
        if (line[j] === '"') {
          if (j + 1 < line.length && line[j + 1] === '"') {
            value += '"';
            j += 2;
          } else {
            j++;
            break;
          }
        } else {
          value += line[j];
          j++;
        }
      }
      fields.push(value);
      i = j + 1; // skip comma
    } else {
      const comma = line.indexOf(",", i);
      if (comma === -1) {
        fields.push(line.slice(i));
        break;
      } else {
        fields.push(line.slice(i, comma));
        i = comma + 1;
      }
    }
  }
  return fields;
}

export async function GET() {
  const csvPath = findLatestFile("joined.csv");

  if (!csvPath) {
    return NextResponse.json(
      { error: "No joined.csv found. Run the forecast_setup notebook first." },
      { status: 404 },
    );
  }

  const content = fs.readFileSync(csvPath, "utf-8");
  const rows = parseCsvToJson(content);
  return NextResponse.json(rows);
}
