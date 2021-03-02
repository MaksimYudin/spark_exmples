package Entity;

import java.io.Serializable;
import java.sql.Date;

public class Car implements Serializable {
    private String name;
    private Double Miles_per_Gallon;
    private Long Cylinders;
    private Double Displacement;
    private Long Horsepower;
    private Long Weight_in_lbs;
    private Double Acceleration;
    private String Year;
    private String Origin;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getMiles_per_Gallon() {
        return Miles_per_Gallon;
    }

    public void setMiles_per_Gallon(Double miles_per_Gallon) {
        Miles_per_Gallon = miles_per_Gallon;
    }

    public Long getCylinders() {
        return Cylinders;
    }

    public void setCylinders(Long cylinders) {
        Cylinders = cylinders;
    }

    public Double getDisplacement() {
        return Displacement;
    }

    public void setDisplacement(Double displacement) {
        Displacement = displacement;
    }

    public Long getHorsepower() {
        return Horsepower != null ? Horsepower : 0;
    }

    public void setHorsepower(Long horsepower) {
        Horsepower = horsepower;
    }

    public Long getWeight_in_lbs() {
        return Weight_in_lbs;
    }

    public void setWeight_in_lbs(Long weight_in_lbs) {
        Weight_in_lbs = weight_in_lbs;
    }

    public Double getAcceleration() {
        return Acceleration;
    }

    public void setAcceleration(Double acceleration) {
        Acceleration = acceleration;
    }

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        Year = year;
    }

    public String getOrigin() {
        return Origin;
    }

    public void setOrigin(String origin) {
        Origin = origin;
    }
}