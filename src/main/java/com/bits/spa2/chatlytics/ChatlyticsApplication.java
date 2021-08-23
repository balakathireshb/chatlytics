package com.bits.spa2.chatlytics;

import com.bits.spa2.chatlytics.process.activeusers.UserCountProcessor;
import com.bits.spa2.chatlytics.process.grouptrends.GroupsTrendProcessor;
import com.bits.spa2.chatlytics.process.msgcount.MessageCountProcessor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ChatlyticsApplication implements CommandLineRunner {

	@Autowired
	private UserCountProcessor ucp;

	@Autowired
	private GroupsTrendProcessor gtp;

	@Autowired
	private MessageCountProcessor mcp;

	public static void main(String[] args) {
		SpringApplication.run(ChatlyticsApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		System.out.println("running");
		ucp.process();
		gtp.process();
		mcp.process();

	}

}
