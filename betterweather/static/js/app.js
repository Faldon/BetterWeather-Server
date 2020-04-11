(function (exports) {

  var that, el, is_table, is_panel, is_card

  var BetterWeather = function BetterWeather (elem_id, options) {
    that = this
    that.init(elem_id, options)
  }

  BetterWeather.UnitSystem = Object.freeze({
    METRIC: 1,
    IMPERIAL: 2
  })

  var default_config = {
    units: BetterWeather.UnitSystem.METRIC,
    wait_cls: 'triple-spinner'
  }

  var toCelsius = function (temperature) {
    return Math.round(temperature - 273.15)
  }

  var toFahrenheit = function (temperature) {
    return Math.round(temperature * 9 / 5 - 459.67)
  }

  var toKmph = function (mps) {
    return Math.round(mps * 3.6)
  }

  var toMph = function (mps) {
    return Math.round(mps * 2.2367)
  }

  var prepareCard = function () {
    var header = el.getElementsByClassName('card-header').item(0) ||
      el.appendChild(document.createElement('div'))
    header.classList.add('card-header', 'bw-header-c')
    var body = el.getElementsByClassName('card-body').item(0) ||
      el.appendChild(document.createElement('div'))
    body.classList.add('card-body', 'bw-body-c')
    if (0 == body.childElementCount) {
      var temperature = document.createElement('div'),
        precipitation = document.createElement('div'),
        windspeed = document.createElement('div'),
        present = document.createElement('div')

      temperature.classList.add('bw-ttt')
      precipitation.classList.add('bw-wwp')
      windspeed.classList.add('bw-ff')
      present.classList.add('bw-present_weather')

      body.appendChild(temperature).setAttribute('id', 'bw-ttt-0')
      body.appendChild(precipitation).setAttribute('id', 'bw-wwp-0')
      body.appendChild(windspeed).setAttribute('id', 'bw-ff-0')
      body.appendChild(present).setAttribute('id', 'bw-present_weather-0')
    }
  }

  BetterWeather.prototype.init = function (elem_id, options) {
    that.config = Object.assign(default_config, options)
    el = document.getElementById(elem_id)
    el.style.setProperty('display', 'none')
    is_table = ~el.nodeName.toLowerCase().indexOf('table')
    is_panel = !!~el.getAttribute('class').indexOf('panel')
    is_card = !!~el.getAttribute('class').indexOf('card')
    el.classList.add('bw-container')
  }

  BetterWeather.prototype.extend = function extend (prop, val) {
    if (typeof val === 'function') {
      BetterWeather.prototype[prop] = val
    } else {
      BetterWeather[prop] = val
    }
  }

  BetterWeather.prototype.getContainer = function () {
    return el
  }

  BetterWeather.prototype.format = function (unit, value) {
    if ('%' == unit) { return value + '%' }
    switch (that.config.units) {
      case BetterWeather.UnitSystem.METRIC:
        if ('K' == unit) { return toCelsius(value) + '°C' }
        if ('m/s' == unit) { return toKmph(value) + 'km/h' }
        break
      case BetterWeather.UnitSystem.IMPERIAL:
        if ('K' == unit) { return toFahrenheit(value) + '°F' }
        if ('m/s' == unit) { return toMph(value) + 'mi/h' }
        break
      default:
        break
    }
    return value
  }

  BetterWeather.prototype.showForecast = function (url) {
    var req = new XMLHttpRequest()
    req.open('GET', url, true)
    req.responseType = 'json'
    req.onload = function () {
      if (req.readyState != 4 || req.status != 200) {
        console.log(req.statusText)
      } else {
        data = req.response
        if (is_table) {
          return
        } else if (is_panel) {
          return
        } else if (is_card) {
          prepareCard()
          el.getElementsByClassName('bw-header-c').item(0).innerHTML =
            'Current weather (Station ' + data.station.id
            + ', ' + data.station.name + ')'
        }
        delete data.station
        data = data.forecasts || [data]
        for (var i = 0; i < data.length; i++) {
          Object.getOwnPropertyNames(data[i]).forEach(function (prop) {
            var node = document.getElementById('bw-' + prop + '-' + i)
            if (node) {
              node.classList.add('bw-' + prop)
              node.innerHTML = typeof data[i][prop] == 'object' ? that.format(
                data[i][prop].unit, data[i][prop].value) : that.format(null,
                data[i][prop])
            }
            if (prop == 'wwp') {
              node.innerHTML += data[i].wwf.value >= data[i].wws.value
                ? '<i id="precipation_type" class="wi wi-raindrops"></i>'
                : '<i id="precipation_type" class="wi wi-snow"></i>'
            }
            if (prop == 'ff') {
              node.innerHTML += '<i id="wind_direction" class="wi wi-wind from-' +
                data[i].dd.value + '-deg"></i>'
            }
          })
        }
        el.style.setProperty('display', 'block')
      }
      Array.from(document.getElementsByClassName(that.config.wait_cls)).forEach(
        function (item) {
          item.style.setProperty('display', 'none')
        }
      )
    }
    req.send()
  }

  exports.BetterWeather = BetterWeather
}((this.window = this.window || {})))