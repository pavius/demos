<template>
  <v-app>
    <v-dialog v-model="configureDialogVisible" width="900">
    </v-dialog>

    <v-container fluid style="padding-top: 10px">
      <v-layout row wrap>
        <v-flex xs6>
          <v-breadcrumbs>
            <v-icon slot="divider">forward</v-icon>
            <v-breadcrumbs-item :disabled="false" to="/">Services</v-breadcrumbs-item>
          </v-breadcrumbs>
        </v-flex>
        <v-flex xs6 style="text-align: right">
          <v-btn color="info" @click.native="showConfigureDialog()" dark class="mb-2">Configure</v-btn>
        </v-flex>
        <v-flex v-for="device in deviceItems" :key="device.id" xs12>
          <v-toolbar flat color="white">
            <v-toolbar-title>{{ device.id }}</v-toolbar-title>
            <v-divider
              class="mx-2"
              inset
              vertical
            ></v-divider>
            <v-chip v-for="label in device.labels"> {{ label }} </v-chip>
          </v-toolbar>
          <v-data-table
            :headers="serviceHeaders"
            :items="device.services"
            hide-actions
            class="elevation-1 text-xs-center"
          >
            <template slot="items" slot-scope="props">
              <tr>
                <td class="text-xs-center">{{ props.item.name }}</td>
                <td class="text-xs-center">{{ getDeviceVersion(props.item.configVersion, props.item.runningVersion) }}</td>
                <td class="text-xs-center">{{ props.item.readyReplicas }}</td>
              </tr>
            </template>
          </v-data-table>
          <br/>
          <br/>

        </v-flex>
      </v-layout>
    </v-container>
  </v-app>
</template>

<script>
export default {
  created () {
    this.refreshInterval = setInterval(function () {
      this.refreshDevices()
    }.bind(this), 1000)
  },
  beforeRouteLeave (to, from, next) {
    clearInterval(this.refreshInterval)
    next()
  },
  data () {
    return {
      deviceItems: [],
      configureDialogVisible: false,
      serviceHeaders: [
        { text: 'Service name', align: 'center' },
        { text: 'Version', align: 'center' },
        { text: 'Replicas', align: 'center' }
      ]
    }
  },
  methods: {

    getDeviceVersion: function (configVersion, runningVersion) {
      if (!configVersion || runningVersion === configVersion) {
        return runningVersion
      }

      return `${runningVersion} => ${configVersion}`
    },

    refreshDevices: function () {
      this.$http.get(process.env.API_URL + '/devices').then(response => {
        let devices = []

        // iterate over devices
        Object.values(response.body).forEach(function (responseDevice) {

          let labels = Object.keys(responseDevice.metadata).map(key => `${key}=${responseDevice.metadata[key]}`)

          let device = {
            id: responseDevice.id,
            labels: labels.sort(),
            services: []
          }

          // get latest device state
          let deviceState = responseDevice.states[0].value
          let deviceConfig = responseDevice.config.value

          // iterate over device states
          Object.keys(deviceState).forEach(function (serviceName) {
            let serviceConfigVersion = ''
            if (deviceConfig && deviceConfig[serviceName] && deviceConfig[serviceName].source) {
              serviceConfigVersion = deviceConfig[serviceName].source.split(':')[1]
            }

            let service = {
              name: serviceName,
              configVersion: serviceConfigVersion,
              runningVersion: deviceState[serviceName].image.split(':')[1],
              readyReplicas: deviceState[serviceName].readyReplicas,
              replicas: deviceState[serviceName].replicas
            }

            device.services.push(service)
          })

          devices.push(device)
        })

        this.deviceItems = devices
        console.log(this.deviceItems)

      }, response => {
        console.warn('Failed to get devices')
      })
    }
  }
}
</script>
