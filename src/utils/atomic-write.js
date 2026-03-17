import fs from 'fs/promises';

/**
 * 原子写入文件 — 先写临时文件再 rename，防止崩溃时数据损坏
 */
export async function atomicWriteJSON(filePath, data) {
  const tmpPath = filePath + '.tmp';
  await fs.writeFile(tmpPath, JSON.stringify(data, null, 2));
  await fs.rename(tmpPath, filePath);
}
