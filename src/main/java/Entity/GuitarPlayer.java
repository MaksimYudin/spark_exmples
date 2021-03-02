package Entity;

public class GuitarPlayer {
    private long id;
    private String name;
    private long[] guitars;
    private long band;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long[] getGuitars() {
        return guitars;
    }

    public void setGuitars(long[] guitars) {
        this.guitars = guitars;
    }

    public long getBand() {
        return band;
    }

    public void setBand(long band) {
        this.band = band;
    }
}
