package uni.fmi.Utils;

public class UtilHelper {
	public static String[] filterFields(String[] fields) {
		for (int i = 0; i < fields.length; i++) {
			fields[i] = fields[i].replace("\"", "");
		}
		return fields;
	}

	public static Double toDouble(String stringValue) {
		Double value = 0d;
		try {
			value = Double.parseDouble(stringValue);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return value;
	}

	public static Integer toInteger(String stringValue) {
		Integer value = 0;
		try {
			value = Integer.parseInt(stringValue);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return value;
	}
}
