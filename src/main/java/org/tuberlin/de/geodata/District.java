package org.tuberlin.de.geodata;


/**
 * Created by gerrit on 22.10.15.
 */
public class District {
    public String borough;
    public String neighborhood;
    public Coordinate[] geometry;

    @Override
    public String toString() {
        String tostring = borough + ";" + neighborhood;

        for (int i = 0; i < geometry.length; i++) {
            tostring += ";" + geometry[i].toString();
        }

        return tostring;
    }

}

