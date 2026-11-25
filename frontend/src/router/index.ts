import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import RebuildDetailView from '@/views/RebuildDetailView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/rebuild_backfill/detail/:backfillId?',
      name: 'rebuild-backfill-detail',
      component: RebuildDetailView,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
