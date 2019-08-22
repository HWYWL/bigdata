package com.yi.demo2.sort.entity;

public class Sort {
    private  String first;
    private  Integer second;

    public Sort() {

    }

    public Sort(String first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return "Sort{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }
}
