import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class H1BCounter {

    public static final String delimiter = ";";

    private void processInputFile(String inputFilePath) throws Exception {

        List<RowInfo> inputList;

        InputStream inputFS;

        int  totalNoOfCertifications = 0;

// Reads the file using Java IO Decorators
        try {

            File inputFile = new File(inputFilePath);

             inputFS = new FileInputStream(inputFile);
        }

        catch ( IOException e) {
            throw new Exception(e.getMessage(), e.getCause());
        }

            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));

            // Skips the header of the csv file as we only need data.
            // Uses mapToRowInfo function defined to create a list of RowInfo object.
            inputList = br.lines().skip(1).map(mapToRowInfo).collect(Collectors.toList());

            br.close();

            // Generation of top_10_states report
            Map<String, Integer> stateCertsMap = new HashMap<>();

            for ( RowInfo item : inputList) {

                String state = item.getState();

                if ("CERTIFIED".equals(item.getStatus())) {

                    stateCertsMap.compute(state, (Key, oldValue) -> oldValue == null ? 1 : oldValue + 1);

                    totalNoOfCertifications++;
                }

            }

            final int totalCerts = totalNoOfCertifications;

            Stream<Map.Entry<String, Integer>> sortedStateCertsStream =
                    stateCertsMap.entrySet().stream()
                            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).limit(10);

           Map<String, Integer>  topStateCertsMap = sortedStateCertsStream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Files.write(Paths.get("./output/top_10_states.txt"), () -> topStateCertsMap.entrySet().stream()
                    .<CharSequence>map(e -> e.getKey() + ";" + e.getValue() +  ";" + e.getValue()/totalCerts + "%")
                    .iterator());
// Add header to the report we already generated
        RandomAccessFile f1 = new RandomAccessFile(new File("./output/top_10_states.txt"), "rw");
        f1.seek(0); // Seeks the very beginning of the file opened
        f1.write("TOP_STATES;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE".getBytes());
        f1.close();

            // Generation of top_10_occupations report

            Map<String, Integer> occupationCertsMap = new HashMap<>();

            for ( RowInfo item : inputList) {

                String occupation = item.getOccupation();

                if ("CERTIFIED".equals(item.getStatus())) {

                    occupationCertsMap.compute(occupation, (Key, oldValue) -> oldValue == null ? 1 : oldValue + 1);
                }
            }

                Stream<Map.Entry<String, Integer>> sortedOccupationCertsStream =
                        occupationCertsMap.entrySet().stream()
                                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).limit(10);

                Map<String, Integer>  topOccupationCertsMap = sortedOccupationCertsStream.collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue));

                Files.write(Paths.get("./output/top_10_occupations.txt"), () -> topOccupationCertsMap.entrySet().stream()
                        .<CharSequence>map(e -> e.getKey() + ";" + e.getValue() +  ";" + e.getValue()/totalCerts + "%")
                        .iterator());

        // Add header to the report we already generated
        RandomAccessFile f2 = new RandomAccessFile(new File("./output/top_10_occupations.txt"), "rw");
        f2.seek(0);  // Seeks the very beginning of the file opened
        f2.write("TOP_OCCUPATIONS;NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE".getBytes());
        f2.close();


    }

    private Function<String, RowInfo> mapToRowInfo = (line) -> {

        String[] p = line.split(delimiter); // The input CSV has semicolon (;) separated lines

        RowInfo rowInfo = new RowInfo();

//      Example from file:  EMPLOYER STATE = Location 50
        if (p[50] != null && p[50].trim().length() > 0) {
            rowInfo.setState(p[50]);
        }
//      Example from file:  CASE_STATUS = CERTIFIED  - LOCATION 2
        if (p[2] != null && p[2].trim().length() > 0) {
            rowInfo.setStatus(p[2]);
        }

//      Example from file:  OCCUPATION  "SOFTWARE DEVELOPERS, APPLICATIONS"; Location 24
        if (p[24] != null && p[24].trim().length() > 0) {
            rowInfo.setOccupation(p[24]);
        }

        return rowInfo;

    };

}

class RowInfo {

    String state;
    String occupation;
    String status;

    public RowInfo() {
    }

    public RowInfo(String state, String occupation, String status) {
        this.state = state;
        this.occupation = occupation;
        this.status = status;
    }

    public String getState() {
        return state;
    }

    public String getOccupation() {
        return occupation;
    }

    public String getStatus() {
        return status;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}