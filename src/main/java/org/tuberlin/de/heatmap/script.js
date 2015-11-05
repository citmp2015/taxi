var map, heatmap;

function toggleHeatmap() {
    heatmap.setMap(heatmap.getMap() ? null : map);
}

function getPoints(callback) {
    $.getJSON('geodata.json', function (data) {

        var geohashs = {};
        $.each(data, function (index, val) {
            var hash = Geohash.encode(val[0], val[1], 6);
            geohashs[hash] = geohashs[hash] ? geohashs[hash] + 1 : 1;
        });

        var points = [];
        $.each(geohashs, function (hash, weight) {
            var point = Geohash.decode(hash);
            points.push({
                location: new google.maps.LatLng(point.lat, point.lon),
                weight: weight
            });
        });

        console.log('%s points grouped in %s geohashs', data.length, Object.keys(geohashs).length);
        callback(null, points);

    }).fail(function (error) {
        callback(error);
    });
}

$(document).ready(function () {

    map = new google.maps.Map($('.map-container')[0], {
        zoom: 12,
        center: {lat: 40.726446, lng: -73.991547},
        mapTypeId: google.maps.MapTypeId.SATELLITE
    });

    getPoints(function (err, points) {

        if (err) return console.error(err);

        heatmap = new google.maps.visualization.HeatmapLayer({
            data: points,
            map: map,
            //radius: 16
            opacity: 1
        });

        console.log('heatmap ready');

    });

});
