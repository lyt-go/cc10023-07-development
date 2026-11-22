import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  await saveBlob(await response.blob(), `${key}.csv`)
}

async function saveBlob(blob: Blob, filename: string): Promise<void> {
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

// ── 分片交付 ────────────────────────────────────────────────
export type ShardManifest = {
  ok: boolean
  batch_id: number
  model_version: string
  expected_partitions: string[]
  covered_partitions: string[]
  missing_partitions: string[]
  coverage_range: string
  complete: boolean
  full_coverage: boolean
  expected_count: number
  completed_count: number
  row_count: number
  duplicate_row_count: number
  message?: string
}

export type DeliveryVersion = {
  id: number
  version_label: string
  status: string
  full_coverage: boolean
  model_version: string
  coverage_range: string
  covered_partitions: string[]
  missing_partitions: string[]
  row_count: number
  created_at: string
}

export async function buildShardManifest(
  taskId: number,
  modelVersion?: string,
): Promise<ShardManifest> {
  const query = modelVersion ? `?model_version=${encodeURIComponent(modelVersion)}` : ''
  const response = await request(`/api/predict_batch/${taskId}/manifest${query}`)
  if (!response.ok) throw new Error('分片清单读取失败')
  return await response.json()
}

export async function listDeliveryVersions(taskId: number): Promise<DeliveryVersion[]> {
  const response = await request(`/api/predict_batch/${taskId}/versions`)
  if (!response.ok) throw new Error('交付版本读取失败')
  const payload = await response.json()
  return payload.versions ?? []
}

export async function publishDeliveryVersion(
  taskId: number,
  options: { requireFull?: boolean } = {},
): Promise<{ ok: boolean; message: string; version?: DeliveryVersion }> {
  const response = await request(`/api/predict_batch/${taskId}/versions`, {
    method: 'POST',
    body: JSON.stringify({ require_full: Boolean(options.requireFull) }),
  })
  return await response.json()
}

export async function archiveDeliveryVersion(
  versionId: number,
): Promise<{ ok: boolean; message: string }> {
  const response = await request(`/api/predict_batch/versions/${versionId}/archive`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return await response.json()
}

export async function reopenDeliveryVersion(
  versionId: number,
): Promise<{ ok: boolean; message: string }> {
  const response = await request(`/api/predict_batch/versions/${versionId}/reopen`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return await response.json()
}

export async function downloadDeliveryVersion(
  versionId: number,
  scope: 'spot' | 'full',
): Promise<void> {
  const response = await request(
    `/api/predict_batch/versions/${versionId}/export.csv?scope=${scope}`,
  )
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    throw new Error(detail?.detail ?? '预测分片下载失败')
  }
  const label = response.headers.get('X-Version-Label') ?? `version-${versionId}`
  await saveBlob(await response.blob(), `prediction_${label}_${scope}.csv`)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
