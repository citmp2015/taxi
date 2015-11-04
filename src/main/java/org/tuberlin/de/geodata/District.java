package org.tuberlin.de.geodata;


import java.util.Arrays;

/**
 * Created by gerrit on 22.10.15.
 */
public class District {
    public String district;
    public Coordinate[] geometry;

    @Override
    public String toString() {
        String tostring = district;

        for (int i = 0; i < geometry.length; i++) {
            tostring += "," + geometry[i].x + "," + geometry[i].y;
        }

        return tostring;
    }

}

