package traze;

import org.apache.commons.lang.builder.ToStringBuilder;

public class PlayerJava {
    private  int owned;
    private  String color;
    private  String name;
    private  int id;
    private  int frags;

    public PlayerJava(){};

    public int getOwned() {
        return owned;
    }

    public void setOwned(int owned) {
        this.owned = owned;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getFrags() {
        return frags;
    }

    public void setFrags(int frags) {
        this.frags = frags;
    }

    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(this);
    }
}