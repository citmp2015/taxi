package org.tuberlin.de.geodata;


import com.vividsolutions.jts.geom.Coordinate;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by gerrit on 22.10.15.
 */
public class District implements Serializable {

    public Coordinate[] geometry;
    public String district;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof District)) return false;

        District district1 = (District) o;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(geometry, district1.geometry)) return false;
        return !(district != null ? !district.equals(district1.district) : district1.district != null);

    }

    @Override
    public int hashCode() {
        int result = geometry != null ? Arrays.hashCode(geometry) : 0;
        result = 31 * result + (district != null ? district.hashCode() : 0);
        return result;
    }

}

