package cn.bisxt.Util;

import java.awt.Image;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;

import javax.imageio.ImageIO;





public class GameUtil {
	private GameUtil() {}//
	public static Image getImage(String path) {
		//将图片加载到内存中 否则读不出长宽
		URL u= GameUtil.class.getClassLoader().getResource(path);
		BufferedImage img = null;
		try {
			img = ImageIO.read(u);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return img;
	}
}
