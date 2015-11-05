var _ = require('underscore');
var csv = require('csv');
var fs = require('fs');

var path = process.argv[2] || 'C:\\Users\\Markus\\Google Drive\\Documents\\Uni\\Verteilte Systeme\\Taxi\\sorted_data.csv\\sorted_data.csv';
if (!path) {
    throw new Error('No path defined');
}

var geodata = [];
var parser = csv.parse({delimiter: ','}, function (err, data) {

    data.forEach(function (row) {

        //0 medallion
        //1 hack_license
        //2 pickup_datetime
        //3 dropoff_datetime
        //4 trip_time_in_secs
        //5 trip_distance
        //6 pickup_longitude
        //7 pickup_latitude
        //8 dropoff_longitude
        //9 dropoff_latitude
        //10 payment_type
        //11 fare_amount
        //12 surcharge
        //13 mta_tax
        //14 tip_amount
        //15 tolls_amount
        //16 total_amount

        var pickupLat = parseFloat(row[7]);
        var pickupLng = parseFloat(row[6]);
        if (!_.isNaN(pickupLat) && !_.isNaN(pickupLng)) {
            geodata.push([pickupLat, pickupLng]);
        }

        var dropoffLat = parseFloat(row[9]);
        var dropoffLng = parseFloat(row[8]);
        if (!_.isNaN(dropoffLat) && !_.isNaN(dropoffLng)) {
            geodata.push([dropoffLat, dropoffLng]);
        }

    });

    var wstream = fs.createWriteStream('geodata.json');
    wstream.write(JSON.stringify(geodata));
    wstream.end();

    console.log('file written to disk');

});

fs.createReadStream(path).pipe(parser);