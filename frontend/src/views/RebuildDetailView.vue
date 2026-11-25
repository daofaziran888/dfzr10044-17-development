<template>
  <section class="detail-page" data-page="rebuild-backfill-detail">
    <p><RouterLink class="link" to="/rebuild_backfill">← 返回重建补数工作台</RouterLink></p>
    <RebuildBackfillPanel :initial-id="backfillId" />
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'

import RebuildBackfillPanel from '@/components/RebuildBackfillPanel.vue'

const route = useRoute()
const backfillId = computed(() =>
  typeof route.params.backfillId === 'string' ? route.params.backfillId : undefined,
)
</script>

<style scoped>
.detail-page { display: grid; gap: 8px; }
</style>
