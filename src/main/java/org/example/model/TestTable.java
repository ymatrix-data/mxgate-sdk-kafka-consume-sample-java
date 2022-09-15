package org.example.model;

import java.io.Serializable;

/**
 *             table "public.test"
 *  Column |            Type             | Collation | Nullable | Default
 * --------+-----------------------------+-----------+----------+---------
 *  ts     | timestamp without time zone |           |          |
 *  tag    | integer                     |           | not null |
 *  c1     | double precision            |           |          |
 *  c2     | double precision            |           |          |
 *  c3     | double precision            |           |          |
 *  c4     | double precision            |           |          |
 *  c5     | text                        |           |          |
 *  c6     | text                        |           |          |
 *  c7     | text                        |           |          |
 *  c8     | text                        |           |          |
 */
public class TestTable implements Serializable {
    private String ts;
    private int tag;
    private double c1;
    private double c2;
    private double c3;
    private double c4;
    private String c5;
    private String c6;
    private String c7;
    private String c8;

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public int getTag() {
        return tag;
    }

    public void setTag(int tag) {
        this.tag = tag;
    }

    public double getC1() {
        return c1;
    }

    public void setC1(double c1) {
        this.c1 = c1;
    }

    public double getC2() {
        return c2;
    }

    public void setC2(double c2) {
        this.c2 = c2;
    }

    public double getC3() {
        return c3;
    }

    public void setC3(double c3) {
        this.c3 = c3;
    }

    public double getC4() {
        return c4;
    }

    public void setC4(double c4) {
        this.c4 = c4;
    }

    public String getC5() {
        return c5;
    }

    public void setC5(String c5) {
        this.c5 = c5;
    }

    public String getC6() {
        return c6;
    }

    public void setC6(String c6) {
        this.c6 = c6;
    }

    public String getC7() {
        return c7;
    }

    public void setC7(String c7) {
        this.c7 = c7;
    }

    public String getC8() {
        return c8;
    }

    public void setC8(String c8) {
        this.c8 = c8;
    }
}
