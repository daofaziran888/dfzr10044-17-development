<template>
  <section class="bf-panel" data-testid="backfill-detail-panel">
    <header class="bf-head">
      <div>
        <h3>重建补数 · 按实际输入批量重建分区清单</h3>
        <p class="bf-tip">
          一次提交多个日期补数，只替换确实取到补件的日期并出具逐条回执；丢件日期沿用旧分区并列为待办。
          与在途增量写入冲突时，清单按「补数 / 增量」拆分范围，已确认输入不丢失；同一补数编号可恢复对账。
        </p>
      </div>
      <RouterLink
        v-if="backfillId"
        class="btn ghost"
        :to="`/rebuild_backfill/detail/${encodeURIComponent(backfillId)}`"
        data-testid="bf-open-page"
      >
        打开补数详情页
      </RouterLink>
    </header>

    <div class="bf-controls">
      <label class="bf-field">
        <span>所属湖表</span>
        <input v-model="table" data-testid="bf-table" placeholder="例如 demo_backfill_0103" />
      </label>
      <label class="bf-field">
        <span>补数编号</span>
        <input v-model="backfillId" data-testid="bf-id" placeholder="例如 BLF-20261103-01" />
      </label>
      <div class="bf-dates" data-testid="bf-date-inputs">
        <div v-for="(row, idx) in dateRows" :key="idx" class="bf-date-row">
          <input v-model="row.date" class="bf-date-key" :data-testid="`bf-date-${row.date || idx}`" placeholder="日期(如 01)" />
          <input
            v-model="row.rows"
            class="bf-date-rows"
            type="number"
            min="0"
            :disabled="row.missing"
            :data-testid="`bf-rows-${row.date || idx}`"
            placeholder="实际行数"
          />
          <label class="bf-missing">
            <input type="checkbox" v-model="row.missing" :data-testid="`bf-missing-${row.date || idx}`" />
            补件丢失
          </label>
          <button class="link" type="button" @click="removeRow(idx)">删除</button>
        </div>
        <button class="btn ghost" type="button" data-testid="bf-add-date" @click="addRow">+ 增加日期</button>
      </div>
      <div class="bf-actions">
        <button class="btn primary" type="button" data-testid="bf-submit-batch" :disabled="busy" @click="submitBatch">
          按实际输入批量重建
        </button>
        <button class="btn" type="button" data-testid="bf-recover" :disabled="busy" @click="recover">
          按补数编号恢复 / 对账
        </button>
        <button class="btn" type="button" data-testid="bf-query-detail" :disabled="busy" @click="queryDetail">
          查询补数详情
        </button>
        <button class="btn ghost" type="button" data-testid="bf-load-demo" :disabled="busy" @click="loadDemo">
          载入题述场景（旧 01=3 / 02=4 / 03=5）
        </button>
      </div>
      <p v-if="errorMessage" class="error-text" data-testid="bf-error">{{ errorMessage }}</p>
    </div>

    <div v-if="loading" class="bf-loading" data-testid="bf-loading">正在生成批量回执…</div>

    <div v-if="detail && detail.found !== false" class="bf-detail" data-testid="bf-detail">
      <div class="bf-summary">
        <div class="bf-block">
          <h4>本次覆盖日期（已确认替换）</h4>
          <div class="bf-chips" data-testid="bf-covered-dates">
            <span
              v-for="d in coveredDates"
              :key="`c-${d}`"
              class="chip chip-ok"
              :data-testid="`bf-covered-${d}`"
            >{{ d }}</span>
            <span v-if="!coveredDates.length" class="bf-none">无</span>
          </div>
          <span class="bf-count" data-testid="bf-covered-count">{{ coveredDates.length }} 个日期已重建</span>
        </div>
        <div class="bf-block">
          <h4>待办 / 缺口日期（仍欠补件，沿用旧分区）</h4>
          <div class="bf-chips" data-testid="bf-pending-dates">
            <span
              v-for="d in pendingDates"
              :key="`p-${d}`"
              class="chip chip-todo"
              :data-testid="`bf-pending-${d}`"
            >{{ d }}</span>
            <span v-if="!pendingDates.length" class="chip chip-ok">全部补齐</span>
          </div>
          <span class="bf-count" data-testid="bf-pending-count">{{ pendingDates.length }} 个日期待补</span>
        </div>
        <div class="bf-block bf-reconcile">
          <h4>恢复对账</h4>
          <p data-testid="bf-reconciled">
            <span class="chip" :class="detail.reconciled || detail.idempotent ? 'chip-ok' : 'chip-muted'">
              {{ detail.recovered ? (detail.idempotent ? '同一补数编号 · 已确认替换可对账（幂等）' : '已恢复并对账') : '首次提交' }}
            </span>
          </p>
          <p class="bf-message" data-testid="bf-message">{{ detail.message }}</p>
        </div>
      </div>

      <h4>按日期逐条核对（补数 vs 增量范围）</h4>
      <table class="data-table bf-items" data-testid="bf-items">
        <thead>
          <tr>
            <th>日期</th>
            <th>原分区行数</th>
            <th>补数实际行数</th>
            <th>增量行数</th>
            <th>清单核对行数</th>
            <th>状态</th>
            <th>范围 / 回执</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in items" :key="item.date" :data-testid="`bf-item-${item.date}`" :data-status="item.status">
            <td>{{ item.date }}</td>
            <td data-testid="bf-item-old">{{ item.old_rows ?? '—' }}</td>
            <td data-testid="bf-item-backfill">{{ item.backfill_rows ?? item.input_rows ?? '—' }}</td>
            <td data-testid="bf-item-inc">{{ item.incremental_rows || 0 }}</td>
            <td data-testid="bf-item-rows"><strong>{{ item.rows }}</strong></td>
            <td>
              <span class="chip" :class="item.confirmed_replacement ? 'chip-ok' : 'chip-todo'">
                {{ item['状态'] || item.status }}
              </span>
              <span v-if="item.conflict" class="chip chip-conflict" data-testid="bf-item-conflict">冲突已拆分</span>
            </td>
            <td class="bf-scope">
              <div v-if="(item.incremental_receipts || []).length" data-testid="bf-item-inc-receipts">
                <span
                  v-for="rc in item.incremental_receipts"
                  :key="rc.receipt_id || rc.id"
                  class="chip chip-inc"
                >增量回执 {{ rc.receipt_id || rc.id }}</span>
              </div>
              <small v-if="item.missing_reason && !item.confirmed_replacement">{{ item.missing_reason }}</small>
              <small v-else>补数文件 {{ short(item.new_file_id) }}<template v-if="(item.preserved_file_ids || []).length"> · 保留增量文件 {{ item.preserved_file_ids.length }} 个</template></small>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="bf-snap-grid">
        <div class="bf-block" data-testid="bf-incremental-receipt">
          <h4>03 日增量回执关联</h4>
          <template v-if="incReceipt">
            <dl class="bf-kv">
              <dt>回执编号</dt>
              <dd data-testid="bf-inc-receipt-id">{{ incReceipt.receipt_id || incReceipt.id }}</dd>
              <dt>分区</dt>
              <dd>{{ incReceipt.partition_key || incReceipt.date }}</dd>
              <dt>增量行数</dt>
              <dd>{{ incReceipt.rows_written ?? incReceipt.rows }}</dd>
              <dt>确认状态</dt>
              <dd>{{ incReceipt.status }}</dd>
              <dt>落湖快照</dt>
              <dd class="mono" data-testid="bf-inc-snapshot">{{ incReceipt.snapshot_id }}</dd>
            </dl>
          </template>
          <p v-else class="bf-none" data-testid="bf-no-inc">本单暂未关联到增量回执。</p>
        </div>

        <div class="bf-block" data-testid="bf-snapshot-panel">
          <h4>所选快照与时间旅行</h4>
          <label class="bf-field">
            <span>选择快照</span>
            <select v-model="selectedSnapshot" data-testid="bf-snapshot-select" @change="queryDetail">
              <option v-for="s in snapshotOptions" :key="s.snapshot_id" :value="s.snapshot_id">
                {{ s.snapshot_id }}（{{ s.status }}，文件 {{ s.file_count }}）
              </option>
            </select>
          </label>
          <dl class="bf-kv">
            <dt>所选快照号</dt>
            <dd class="mono" data-testid="bf-selected-snapshot">{{ detail.selected_snapshot_id || detail.snapshot_id }}</dd>
            <template v-if="selectedRead">
              <dt>该快照行数</dt>
              <dd data-testid="bf-selected-rows">{{ selectedRead.rows }}</dd>
              <dt>命中分区数</dt>
              <dd data-testid="bf-selected-partitions">{{ selectedRead.partition_count }}（{{ (selectedRead.partitions_hit || []).join('、') }}）</dd>
            </template>
          </dl>
          <div v-if="oldRange" class="bf-old" data-testid="bf-old-snapshot">
            <h5>按旧快照号回到原范围</h5>
            <p>
              旧快照 <span class="mono" data-testid="bf-old-snapshot-id">{{ detail.old_snapshot_id }}</span>
              ：<strong data-testid="bf-old-rows">{{ oldRange.rows }}</strong> 行 ·
              <strong data-testid="bf-old-partitions">{{ oldRange.partition_count }}</strong> 个分区
              （{{ (oldRange.partitions_hit || []).join('、') }}，即 3 + 4 + 5）
            </p>
            <button class="btn ghost" type="button" data-testid="bf-view-old" @click="viewOldSnapshot">
              在上方“所选快照”中查看旧快照
            </button>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  batchRebuild,
  fetchBackfillDetail,
  loadDemoScenario,
  recoverBackfill,
} from '@/api/backfill'

const props = defineProps<{ initialId?: string }>()

type DateRow = { date: string; rows: string; missing: boolean }

const table = ref('demo_backfill_0103')
const backfillId = ref(props.initialId || 'BLF-DEMO-0103')
const dateRows = ref<DateRow[]>([
  { date: '01', rows: '8', missing: false },
  { date: '02', rows: '', missing: true },
  { date: '03', rows: '6', missing: false },
])
const detail = ref<any>(null)
const selectedSnapshot = ref('')
const loading = ref(false)
const busy = ref(false)
const errorMessage = ref('')

const coveredDates = computed<string[]>(() => detail.value?.covered_dates ?? [])
const pendingDates = computed<string[]>(() => detail.value?.pending_dates ?? [])
const items = computed<any[]>(() => detail.value?.items ?? [])
const snapshotOptions = computed<any[]>(
  () => detail.value?.snapshot_options ?? detail.value?.snapshots ?? [],
)
const selectedRead = computed<any>(() => detail.value?.selected_snapshot_read ?? null)
const oldRange = computed<any>(() => detail.value?.old_range ?? null)
const incReceipt = computed<any>(
  () => detail.value?.partition_incremental_receipt ?? detail.value?.incremental_receipt ?? null,
)

function short(id?: string): string {
  if (!id) return '—'
  return id.length > 18 ? `${id.slice(0, 18)}…` : id
}

function addRow() {
  dateRows.value.push({ date: '', rows: '', missing: false })
}

function removeRow(index: number) {
  dateRows.value.splice(index, 1)
}

function buildPartitions(): Record<string, number> {
  const partitions: Record<string, number> = {}
  for (const row of dateRows.value) {
    if (row.date && !row.missing && row.rows !== '') {
      partitions[row.date] = Number(row.rows)
    }
  }
  return partitions
}

function buildDates(): string[] {
  return dateRows.value.map((row) => row.date).filter(Boolean)
}

function absorb(data: any) {
  detail.value = data
  if (data?.lake_table) table.value = data.lake_table
  if (data?.backfill_id) backfillId.value = data.backfill_id
  selectedSnapshot.value = data?.selected_snapshot_id || data?.snapshot_id || ''
}

async function submitBatch() {
  busy.value = true
  errorMessage.value = ''
  try {
    const data = await batchRebuild({
      lake_table: table.value,
      backfill_id: backfillId.value,
      dates: buildDates(),
      partitions: buildPartitions(),
    })
    if (data?.ok === false) throw new Error(data.message || '批量重建失败')
    absorb(data)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量重建失败'
  } finally {
    busy.value = false
  }
}

async function recover() {
  busy.value = true
  errorMessage.value = ''
  try {
    const data = await recoverBackfill(backfillId.value, {
      lake_table: table.value,
      dates: buildDates(),
      partitions: buildPartitions(),
    })
    if (data?.ok === false) throw new Error(data.message || '恢复对账失败')
    absorb(data)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复对账失败'
  } finally {
    busy.value = false
  }
}

async function queryDetail() {
  loading.value = true
  errorMessage.value = ''
  try {
    const data = await fetchBackfillDetail(backfillId.value, {
      snapshotId: selectedSnapshot.value || undefined,
    })
    if (data?.found === false) throw new Error(data.message || '未找到补数回执')
    absorb(data)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '查询详情失败'
  } finally {
    loading.value = false
  }
}

async function loadDemo() {
  busy.value = true
  loading.value = true
  errorMessage.value = ''
  try {
    const data = await loadDemoScenario()
    absorb(data)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '题述场景载入失败（后端是否已启动？）'
  } finally {
    busy.value = false
    loading.value = false
  }
}

function viewOldSnapshot() {
  const oldId = detail.value?.old_snapshot_id || detail.value?.base_snapshot_id
  if (oldId) {
    selectedSnapshot.value = oldId
    void queryDetail()
  }
}

onMounted(async () => {
  if (props.initialId) {
    await queryDetail()
  } else {
    await loadDemo()
  }
})
</script>

<style scoped>
.bf-panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
  margin: 12px 0 16px;
}
.bf-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }
.bf-head h3 { margin: 0 0 4px; font-size: 16px; }
.bf-tip { color: var(--muted); font-size: 12.5px; line-height: 1.7; margin: 0; max-width: 920px; }
.bf-controls { margin: 12px 0; display: grid; gap: 10px; }
.bf-field { display: grid; gap: 4px; font-size: 12px; color: var(--muted); }
.bf-field input, .bf-field select {
  border: 1px solid var(--border); border-radius: 6px; padding: 7px 9px; font-size: 13px; color: #1f2937;
}
.bf-controls > .bf-field { max-width: 360px; }
.bf-dates { display: grid; gap: 6px; }
.bf-date-row { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.bf-date-key { width: 120px; border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; }
.bf-date-rows { width: 140px; border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; }
.bf-missing { font-size: 12px; color: var(--muted); display: flex; align-items: center; gap: 4px; }
.bf-actions { display: flex; gap: 8px; flex-wrap: wrap; }
.bf-loading { color: var(--brand); font-size: 13px; padding: 8px 0; }
.bf-detail { border-top: 1px dashed var(--border); padding-top: 12px; display: grid; gap: 12px; }
.bf-summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }
.bf-block { background: #f8fafc; border: 1px solid #eef2f7; border-radius: 8px; padding: 10px 12px; }
.bf-block h4 { margin: 0 0 8px; font-size: 13px; }
.bf-block h5 { margin: 8px 0 4px; font-size: 12.5px; }
.bf-chips { display: flex; gap: 6px; flex-wrap: wrap; }
.chip {
  display: inline-block; border-radius: 999px; padding: 2px 10px; font-size: 12px;
  border: 1px solid var(--border); background: #fff; color: #334155;
}
.chip-ok { background: #ecfdf3; border-color: #abefc6; color: #067647; }
.chip-todo { background: #fffaeb; border-color: #fedf89; color: #b54708; }
.chip-conflict { background: #fef3f2; border-color: #fda29b; color: #b42318; }
.chip-inc { background: #eff8ff; border-color: #b2ddff; color: #175cd3; margin: 2px 4px 2px 0; }
.chip-muted { background: #f2f4f7; color: #475467; }
.bf-count { display: block; margin-top: 6px; font-size: 12px; color: var(--muted); }
.bf-message { font-size: 12.5px; color: #334155; margin: 6px 0 0; }
.bf-items { font-size: 12.5px; }
.bf-scope small { color: var(--muted); display: block; margin-top: 4px; }
.bf-snap-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }
.bf-kv { display: grid; grid-template-columns: 84px 1fr; gap: 4px 10px; margin: 0; font-size: 12.5px; }
.bf-kv dt { color: var(--muted); }
.bf-kv dd { margin: 0; word-break: break-all; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 11.5px; }
.bf-old { margin-top: 10px; border-top: 1px dashed var(--border); padding-top: 8px; }
.bf-old p { margin: 4px 0 8px; font-size: 12.5px; }
.bf-none { color: var(--muted); font-size: 12.5px; }
.bf-reconcile { display: flex; flex-direction: column; gap: 4px; }
</style>
