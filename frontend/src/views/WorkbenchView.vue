<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <section v-if="meta.key === 'predict_batch'" class="shard-panel" data-testid="shard-panel">
      <header class="shard-head">
        <h3>分片交付</h3>
        <span class="shard-hint">按同批同模型版本合并实际结果行；缺片版本仅可抽查，全量版才可复核。</span>
      </header>
      <div class="filter-bar">
        <label class="filter-item">
          <span>预测任务</span>
          <select v-model.number="shardTaskId" data-testid="shard-task">
            <option v-for="row in rows" :key="String(row.id)" :value="Number(row.id)">
              {{ row['任务编号'] }}（#{{ row.id }}）
            </option>
          </select>
        </label>
        <button class="btn" data-testid="shard-load" type="button" @click="loadManifest">
          读取分片清单
        </button>
        <button class="btn" data-testid="shard-publish-spot" type="button" @click="publish(false)">
          发布抽查版
        </button>
        <button
          class="btn primary"
          data-testid="shard-publish-full"
          type="button"
          :disabled="!manifest?.complete"
          @click="publish(true)"
        >
          发布全量版
        </button>
      </div>

      <p v-if="manifest" class="shard-summary" data-testid="shard-summary">
        模型版本 {{ manifest.model_version }}｜预计 {{ manifest.expected_count }} 片，已收
        {{ manifest.completed_count }} 片｜覆盖 {{ manifest.coverage_range || '无' }}｜
        缺片 {{ manifest.missing_partitions.join(',') || '无' }}｜
        {{ manifest.complete ? '全量标记：是' : '全量标记：否（仅可抽查）' }}
      </p>

      <table v-if="versions.length" class="data-table" data-testid="shard-versions">
        <thead>
          <tr>
            <th>版本</th><th>状态</th><th>全量标记</th><th>模型版本</th>
            <th>覆盖分区</th><th>缺失分区</th><th>行数</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="version in versions" :key="version.id" :data-testid="`version-${version.id}`">
            <td>{{ version.version_label }}</td>
            <td>{{ version.status }}</td>
            <td>{{ version.full_coverage ? '是' : '否' }}</td>
            <td>{{ version.model_version }}</td>
            <td>{{ version.coverage_range || '—' }}</td>
            <td>{{ version.missing_partitions.join(',') || '—' }}</td>
            <td>{{ version.row_count }}</td>
            <td class="row-actions">
              <button class="link" data-testid="download-spot" type="button" @click="download(version, 'spot')">
                下载抽查
              </button>
              <button
                v-if="version.full_coverage"
                class="link"
                data-testid="download-full"
                type="button"
                @click="download(version, 'full')"
              >
                下载全量
              </button>
              <button
                v-if="version.status !== '已归档'"
                class="link"
                data-testid="archive-version"
                type="button"
                @click="archive(version)"
              >
                归档
              </button>
              <button
                v-else
                class="link"
                data-testid="reopen-version"
                type="button"
                @click="reopen(version)"
              >
                重新打开
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  archiveDeliveryVersion,
  buildShardManifest,
  createEntry,
  downloadDeliveryVersion,
  downloadEntries,
  listDeliveryVersions,
  listEntries,
  moduleMeta,
  publishDeliveryVersion,
  reopenDeliveryVersion,
  runAction as applyAction,
} from '@/api/backend-service'
import type { DeliveryVersion, ShardManifest } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const shardTaskId = ref<number>(0)
const manifest = ref<ShardManifest | null>(null)
const versions = ref<DeliveryVersion[]>([])
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
    if (meta.key === 'predict_batch' && !rows.value.some((row) => Number(row.id) === shardTaskId.value)) {
      shardTaskId.value = rows.value.length ? Number(rows.value[0].id) : 0
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

async function loadManifest() {
  if (!shardTaskId.value) return
  errorMessage.value = ''
  try {
    manifest.value = await buildShardManifest(shardTaskId.value)
    versions.value = await listDeliveryVersions(shardTaskId.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分片清单读取失败'
  }
}

async function publish(requireFull: boolean) {
  if (!shardTaskId.value) return
  errorMessage.value = ''
  try {
    const result = await publishDeliveryVersion(shardTaskId.value, { requireFull })
    if (!result.ok) throw new Error(result.message)
    await loadManifest()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '版本发布失败'
  }
}

async function download(version: DeliveryVersion, scope: 'spot' | 'full') {
  errorMessage.value = ''
  try {
    await downloadDeliveryVersion(version.id, scope)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function archive(version: DeliveryVersion) {
  errorMessage.value = ''
  try {
    const result = await archiveDeliveryVersion(version.id)
    if (!result.ok) throw new Error(result.message)
    await loadManifest()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '归档失败'
  }
}

async function reopen(version: DeliveryVersion) {
  errorMessage.value = ''
  try {
    const result = await reopenDeliveryVersion(version.id)
    if (!result.ok) throw new Error(result.message)
    await loadManifest()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重新打开失败'
  }
}

onMounted(reload)
</script>
