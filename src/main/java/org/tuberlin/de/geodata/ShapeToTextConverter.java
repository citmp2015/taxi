package org.tuberlin.de.geodata;

import com.vividsolutions.jts.geom.Geometry;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by gerrit on 05.11.15.
 */
public class ShapeToTextConverter {

    public static void convertShape(String shapePath, String textFileOutputPath) {
        Collection<District> districts = extractDistrictsFromShapefile(shapePath);

        BufferedWriter writer = null;

        File output = new File(textFileOutputPath);
        try {
            writer = new BufferedWriter(new FileWriter(output));

            for (District d : districts) {
                writer.write(d.toString() + "\n");

            }
            writer.flush();


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * @param path to shapefile (extension .shp)
     * @return a collection of districts containing geometries and district names
     */

    private static Collection<District> extractDistrictsFromShapefile(String path) {
        File file = new File(path);
        Map<String, Object> map = new HashMap<String, Object>();
        FeatureSource<SimpleFeatureType, SimpleFeature> source = null;
        DataStore dataStore = null;
        FeatureIterator<SimpleFeature> features = null;
        try {

            map.put("url", file.toURI().toURL());

            dataStore = DataStoreFinder.getDataStore(map);

            String typeName = dataStore.getTypeNames()[0];

            source = dataStore
                    .getFeatureSource(typeName);


            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = null;
            collection = source.getFeatures();


            HashSet<District> feats = new HashSet<District>();
            features = collection.features();
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                District d = new District();
                d.district = (String) feature.getAttribute("label");
                d.geometry = convertGeometry(((Geometry) feature.getDefaultGeometry()).getCoordinates());
                feats.add(d);
            }
            return feats;

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            features.close();
            dataStore.dispose();
        }


        return null;
    }

    private static org.tuberlin.de.geodata.Coordinate[] convertGeometry(com.vividsolutions.jts.geom.Coordinate[] coordinates) {
        org.tuberlin.de.geodata.Coordinate[] converted = new org.tuberlin.de.geodata.Coordinate[coordinates.length];

        for (int i = 0; i < coordinates.length; i++) {
            com.vividsolutions.jts.geom.Coordinate c = coordinates[i];
            org.tuberlin.de.geodata.Coordinate conv = new org.tuberlin.de.geodata.Coordinate();
            conv.x = c.x;
            conv.y = c.y;
            converted[i] = conv;
        }

        return converted;
    }
}


