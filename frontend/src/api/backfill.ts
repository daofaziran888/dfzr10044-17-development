import { request } from './client'

/** 补数批量回执结构宽松，后端给了大量同义字段，前端按存在性读取。 */
export type BackfillReceipt = Record<string, any>

async function parse<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return (await response.json()) as T
}

/** 构造并读取题述标准场景（01=3/02=4/03=5，补到 01=8/03=6，03 含在途增量）。 */
export async function loadDemoScenario(): Promise<BackfillReceipt> {
  const response = await request('/api/rebuild_backfill/demo-scenario', { method: 'POST' })
  return parse<BackfillReceipt>(response)
}

/** 按实际输入多日期批量重建分区，返回逐条批量回执。 */
export async function batchRebuild(body: Record<string, unknown>): Promise<BackfillReceipt> {
  const response = await request('/api/rebuild_backfill/batch-rebuild', {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return parse<BackfillReceipt>(response)
}

/** 以同一补数编号恢复 / 对账，可附带新取到的补件。 */
export async function recoverBackfill(
  backfillId: string,
  body: Record<string, unknown> = {},
): Promise<BackfillReceipt> {
  const response = await request(
    `/api/rebuild_backfill/recover/${encodeURIComponent(backfillId)}`,
    { method: 'POST', body: JSON.stringify(body) },
  )
  return parse<BackfillReceipt>(response)
}

/** 查询补数详情，可指定所选快照与日期。 */
export async function fetchBackfillDetail(
  backfillId: string,
  params: { snapshotId?: string; date?: string } = {},
): Promise<BackfillReceipt> {
  const query = new URLSearchParams({ backfill_id: backfillId })
  if (params.snapshotId) query.set('snapshot_id', params.snapshotId)
  if (params.date) query.set('date', params.date)
  const response = await request(`/api/rebuild_backfill/detail?${query.toString()}`)
  return parse<BackfillReceipt>(response)
}
