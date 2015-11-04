package org.tuberlin.de.geodata;

/**
 * Created by gerrit on 04.11.15.
 */
public class Coordinate {
    public double x;

    @Override
    public String toString() {
        return x + "," + y;
    }

    public double y;
}
