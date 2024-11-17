package uni.fmi.ExoplanetAnalyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import mappers.ExoplanetDistanceFilterMapper;
import mappers.PlanetAnalyzerListMapper;
import mappers.PlanetAnalyzerPlanetCounterMapper;
import uni.fmi.reducers.ExoplanetDistanceFilterReducer;
import uni.fmi.reducers.PlanetAnalyzerPlanetCounterReducer;
import uni.fmi.reducers.PlanetAnalyzerListReducer;

public class App extends Application {

	@FXML
	private ComboBox<String> comboBox;

	@FXML
	private TextField methodTextField;

	@FXML
	private TextField numberTextField;

	@FXML
	private TextField orbitalPeriodTextField;

	@FXML
	private TextField distanceMinTextField;

	@FXML
	private TextField distanceMaxTextField;
	
	@FXML
	private TextArea resultTextArea;

	@FXML
	private Button button;

	private static final String PLANET_LIST = "Planet List";
	private static final String AVERAGE_MASS = "Average Mass";
	private static final String EARTH_DISTANCE = "Earth Distance";
	private static final String HADOOP_URL = "hdfs://127.0.0.1:9000";

	public static void main(String[] args) {
		launch(args);
	}

	@Override
	public void start(Stage stage) throws Exception {
		try {
			Parent root = FXMLLoader.load(getClass().getClassLoader().getResource("big_data_interface.fxml"));
			Scene scene = new Scene(root);
			stage.setScene(scene);
			stage.setTitle("Exoplanet Data Analyzer");
			stage.show();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void runHadoop(String comboBoxValue) {
		Configuration conf = new Configuration();

		Path inputPath = new Path(HADOOP_URL + "/input/24.csv");
		Path outputPath = new Path(HADOOP_URL + "/planetAnalyzerResult");

		JobConf jobConf = new JobConf(conf, App.class);

		if (PLANET_LIST.equals(comboBoxValue)) {
			filterPlanetList(jobConf);
		} else if (AVERAGE_MASS.equals(comboBoxValue)) {
			calculateAverageMass(jobConf);
		}else if (EARTH_DISTANCE.equals(comboBoxValue)) {
			calculateEarthDistanceCount(jobConf);
		}

		FileInputFormat.setInputPaths(jobConf, inputPath);
		FileOutputFormat.setOutputPath(jobConf, outputPath);

		try {
			FileSystem fileSystem = FileSystem.get(URI.create(HADOOP_URL), conf);

			if (fileSystem.exists(outputPath)) {
				fileSystem.delete(outputPath, true);
			}

			RunningJob runningJob = JobClient.runJob(jobConf);
			if (runningJob.isSuccessful()) {
				displayResult(fileSystem);
		    }
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void filterPlanetList(JobConf jobConf) {
		jobConf.setMapperClass(PlanetAnalyzerListMapper.class);
		jobConf.setReducerClass(PlanetAnalyzerListReducer.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		String method = methodTextField.getText().trim();
		String number = numberTextField.getText();
		String orbitalPeriod = orbitalPeriodTextField.getText().trim();
		if (!orbitalPeriod.isEmpty()) {
			try {
				Float.parseFloat(orbitalPeriod);
			} catch (NumberFormatException e) {
				System.err.println("Invalid orbital period value: " + orbitalPeriod);
				return;
			}
		}

		if (!method.isEmpty())
			jobConf.set("method", method);
		if (!number.isEmpty())
			jobConf.set("number", number);
		if (!orbitalPeriod.isEmpty())
			jobConf.set("orbital_period", orbitalPeriod);
	}
	
	private void calculateAverageMass(JobConf jobConf) {
		jobConf.setMapperClass(PlanetAnalyzerPlanetCounterMapper.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setReducerClass(PlanetAnalyzerPlanetCounterReducer.class);
		jobConf.setOutputValueClass(DoubleWritable.class);
	}
	
	private void calculateEarthDistanceCount(JobConf jobConf) {
		jobConf.setMapperClass(ExoplanetDistanceFilterMapper.class);
		jobConf.setReducerClass(ExoplanetDistanceFilterReducer.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		String min = distanceMinTextField.getText().trim();
		String max = distanceMaxTextField.getText().trim();
		if (!min.isEmpty()) {
			try {
				Float.parseFloat(min);
			} catch (NumberFormatException e) {
				System.err.println("Invalid orbital period value: " + min);
				return;
			}
		}

		if (!max.isEmpty()) {
			try {
				Float.parseFloat(max);
			} catch (NumberFormatException e) {
				System.err.println("Invalid orbital period value: " + max);
				return;
			}
		}
		
		if (!min.isEmpty())
			jobConf.set("min", min);
		if (!max.isEmpty())
			jobConf.set("max", max);
	}
	
	private void displayResult(FileSystem fileSystem) throws IOException {
        // Define the result folder path
        Path localResultFolder = new Path("planetAnalyzerResult");
        Path hdfsResultFolder = new Path("/planetAnalyzerResult");

        // Delete the local folder if it exists
        File localFolder = new File("planetAnalyzerResult");
        if (localFolder.exists() && localFolder.isDirectory()) {
            deleteDirectory(localFolder); // Recursively delete the folder
        }

        // Fetch the HDFS result to local
        fileSystem.copyToLocalFile(hdfsResultFolder, localResultFolder);

        // Read the part-00000 file
        File resultFile = new File("planetAnalyzerResult/part-00000");
        if (resultFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(resultFile))) {
                StringBuilder resultBuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    resultBuilder.append(line).append("\n");
                }

                // Set the result to the TextField
                resultTextArea.setText(resultBuilder.toString().trim());
            }
        } else {
            System.out.println("Result file part-00000 not found.");
        }
	}
	
	private static void deleteDirectory(File directory) {
	    File[] allContents = directory.listFiles();
	    if (allContents != null) {
	        for (File file : allContents) {
	            deleteDirectory(file);
	        }
	    }
	    directory.delete();
	}

	@FXML
	public void initialize() {
		comboBox.getItems().addAll(PLANET_LIST, AVERAGE_MASS, EARTH_DISTANCE);
		comboBox.setValue(PLANET_LIST);
		distanceMaxTextField.setDisable(true);
		distanceMinTextField.setDisable(true);
	}

	@FXML
	public void comboBoxOnAction(ActionEvent event) {
		String comboBoxValue = comboBox.getValue();

		if (AVERAGE_MASS.equals(comboBoxValue)) {
			methodTextField.setDisable(true);
			numberTextField.setDisable(true);
			orbitalPeriodTextField.setDisable(true);
			distanceMaxTextField.setDisable(true);
			distanceMinTextField.setDisable(true);
		} else if (PLANET_LIST.equals(comboBoxValue)) {
			methodTextField.setDisable(false);
			numberTextField.setDisable(false);
			orbitalPeriodTextField.setDisable(false);
			distanceMaxTextField.setDisable(true);
			distanceMinTextField.setDisable(true);
		} else if (EARTH_DISTANCE.equals(comboBoxValue)) {
			methodTextField.setDisable(true);
			numberTextField.setDisable(true);
			orbitalPeriodTextField.setDisable(true);
			distanceMaxTextField.setDisable(false);
			distanceMinTextField.setDisable(false);
		}
		
	}

	@FXML
	public void buttonOnAction(ActionEvent event) {
		String comboBoxValue = comboBox.getValue();

		if (comboBoxValue != null) {
			runHadoop(comboBoxValue);
		}
	}

	@FXML
	public void resetAll(ActionEvent event) {
		methodTextField.clear();
		numberTextField.clear();
		orbitalPeriodTextField.clear();
		comboBox.setValue(PLANET_LIST);
		methodTextField.setDisable(false);
		numberTextField.setDisable(false);
		orbitalPeriodTextField.setDisable(false);
		distanceMaxTextField.clear();
		distanceMinTextField.clear();
		resultTextArea.clear();
	}

	@FXML
	public void exitApplication(ActionEvent event) {
		System.exit(0);
	}
}
