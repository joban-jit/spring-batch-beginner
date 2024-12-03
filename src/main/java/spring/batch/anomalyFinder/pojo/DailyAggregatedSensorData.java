package spring.batch.anomalyFinder.pojo;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

@Setter
@XmlRootElement(name = "daily-data")
@NoArgsConstructor
@AllArgsConstructor
public class DailyAggregatedSensorData {
    private String date;
    private double min;
    private double avg;
    private double max;

    // Getters and setters
    @XmlElement
    public String getDate() {
        return date;
    }

    @XmlElement
    public double getMin() {
        return min;
    }

    @XmlElement
    public double getAvg() {
        return avg;
    }

    @XmlElement
    public double getMax() {
        return max;
    }

    public static Jaxb2Marshaller getMarshaller(){
        Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
        jaxb2Marshaller.setClassesToBeBound(DailyAggregatedSensorData.class);
        return jaxb2Marshaller;
    }

}
