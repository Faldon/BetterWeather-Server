function showForecast(url) {
    $.ajax({
        url,
        method: 'GET'
    }).done(function(data, textStatus, jqXHR) {
        $('#forecast_header').prop(
            'innerHTML',
            'Current weather (Station ' + data['station']['id'] + ', ' + data['station']['name'] + ')'
        )
        $('#temperature').prop('innerHTML', '<b>' + toCelsius(data.ttt.value) + '</b>°C');

        if(data.wwf.value >= data.wws.value) {
            $('#precipitation').prop('innerHTML', '<i id="precipation_type" class="wi wi-raindrops"></i>');
        } else {
            $('#precipitation').prop('innerHTML', '<i id="precipation_type" class="wi wi-snow"></i>');
        }
        $('#precipation_type').after(' <b>' + data.wwp.value + '</b>%');

        $('#windspeed').prop(
            'innerHTML',
            '<i id="wind_direction" class="wi wi-wind towards-' + data.dd.value + '-deg"></i> <b>' + toKmph(data.ff.value) + '</b>Km/h'
        );
        $('#present_weather').prop(
            'innerHTML',
            data.present_weather
        );
        $('.triple-spinner').hide();
        $('#forecast').show();
        console.dir(data);
    }).fail(function(jqXHR, textStatus, errorThrown) {
        $('.triple-spinner').hide();
        console.log(errorThrown);
    });
}