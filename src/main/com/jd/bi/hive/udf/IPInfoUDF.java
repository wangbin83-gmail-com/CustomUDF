package com.jd.bi.hive.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.gson.Gson;

public class IPInfoUDF extends UDF {
	private static final String IPServer = "http://ip.jd.com/service/getIpInfo?ip=";

	public String evaluate(String ipStr) {
		StringBuilder sb = new StringBuilder();
		try {
			URL url = new URL(IPServer + ipStr);
			HttpURLConnection connection = (HttpURLConnection) url
					.openConnection();
			connection.connect();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					connection.getInputStream(), "utf-8"));
			String line;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
			reader.close();
			connection.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Gson gson = new Gson();
		IpAddressResult result = gson.fromJson(sb.toString(),
				IpAddressResult.class);
		if (-1 == result.status) {
			return ipStr;
		} else {
			return result.data.getCountry();
		}
	}

	// {"status":1,"data":{"country":"","province":"","city":"","district":"","subdistrict":"","community":"","road":""}}
	@SuppressWarnings("unused")
	private class IpAddressResult {
		private int status;
		private IpAddress data;

		public int getStatus() {
			return status;
		}

		public void setStatus(int status) {
			this.status = status;
		}

		public IpAddress getIp_address() {
			return data;
		}

		public void setIp_address(IpAddress ip_address) {
			this.data = ip_address;
		}
	}

	@SuppressWarnings("unused")
	private class IpAddress {
		private String country;

		public String getCountry() {
			return country;
		}

		public void setCountry(String country) {
			this.country = country;
		}

		public String getProvince() {
			return province;
		}

		public void setProvince(String province) {
			this.province = province;
		}

		public String getCity() {
			return city;
		}

		public void setCity(String city) {
			this.city = city;
		}

		public String getDistrict() {
			return district;
		}

		public void setDistrict(String district) {
			this.district = district;
		}

		public String getSubdistrict() {
			return subdistrict;
		}

		public void setSubdistrict(String subdistrict) {
			this.subdistrict = subdistrict;
		}

		public String getCommunity() {
			return community;
		}

		public void setCommunity(String community) {
			this.community = community;
		}

		public String getRoad() {
			return road;
		}

		public void setRoad(String road) {
			this.road = road;
		}

		private String province;
		private String city;
		private String district;
		private String subdistrict;
		private String community;
		private String road;

	}

	public static void main(String[] args) {
		IPInfoUDF ip_info_udf = new IPInfoUDF();
		System.out.println(ip_info_udf.evaluate("61.135.169.125"));
	}
}
