import Vue from 'vue'
import Router from 'vue-router'
import Devices from '@/components/Devices'
import VueResource from 'vue-resource'

Vue.use(Router)
Vue.use(VueResource)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'Devices',
      component: Devices
    }
  ]
})
