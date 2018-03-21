package cn.bisxt.plane;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.Date;

import cn.bisxt.Util.Constant;
import cn.bisxt.Util.GameUtil;
import cn.bisxt.Util.MyFrame;

public class PlaneGameFrame extends MyFrame{
	Image bg = GameUtil.getImage("images/bg.jpg");
	
	Plane p = new Plane("images/plane.png",50,50);
	ArrayList bulletList = new ArrayList();
	
	Explode bao;
	
	Date startTime;
	Date deadTime;
	@Override
	public void paint(Graphics g) {
		
		g.drawImage(bg, 0, 0, null);
		p.draw(g);
	
		for(int i=0;i<bulletList.size();i++) {
			Bullet b = (Bullet) bulletList.get(i);
			b.draw(g);
			//���ͷɻ�����ײ
			boolean peng = b.getRect().intersects(p.getRect());
			printInfo(g,"ʱ��:"+(int)(new Date().getTime()-startTime.getTime())/1000+"��",20,380,60,Color.WHITE);
			if(peng) {
				p.setLive(false);	
				if(bao==null) {
					deadTime = new Date();
					bao= new Explode(p.x,p.y);
				}
				bao.draw(g);
				
				break;
			}	
		}
		if(!p.isLive()) {
			//printInfo(g,"GameOver",30,100,200,Color.WHITE);
			int period = (int)(deadTime.getTime()-startTime.getTime())/1000;
			printInfo(g,"����ʱ��"+period+"��",20,120,260,Color.WHITE);
			switch(period/10) {
			case 0:
				printInfo(g,"����",50,100,200,Color.white);
				break;
			case 1:
				printInfo(g,"����",50,100,200,Color.white);
				break;
			case 2:
				printInfo(g,"�߼�",50,100,200,Color.white);
				break;
			case 3:
				printInfo(g,"��ʦ",50,100,200,Color.white);
				break;
			default:
				break;
			}
		}
		
	}
	
	//���ڴ�ӡ����
	public void printInfo(Graphics g,String str,int size,int x,int y,Color color) {
		Color c = g.getColor();
		g.setColor(color);
		Font f = new Font("����",Font.BOLD,size);
		g.setFont(f);
		g.drawString(str, x,y);
		g.setColor(c);
	}
	//˫����������˸
	private Image offScreenImage = null;
	public void update(Graphics g) {
		if(offScreenImage == null) {
			offScreenImage = this.createImage(Constant.GAME_WIDTH, Constant.GAME_HEIGHT);
		}
		Graphics goff = offScreenImage.getGraphics();
		paint(goff);
		g.drawImage(offScreenImage, 0, 0, null);
	}
	
	public static void main(String[] args) {
		new PlaneGameFrame().launchFrame();
	}
	
	public void launchFrame() {
		super.launchFrame();
		//���̼���
		addKeyListener(new KeyMonitor());
		//�����ӵ�
		for(int i=0;i<10;i++) {
			Bullet b = new  Bullet();
			bulletList.add(b);
		}
		startTime = new Date();
	}
	
	//����Ϊ�ڲ�����Է����ʹ���ⲿ�������
	class KeyMonitor extends KeyAdapter{
		@Override
		public void keyPressed(KeyEvent e) {
			p.addDirection(e);
			
		}
		@Override
		public void keyReleased(KeyEvent e) {
			p.minusDirection(e);
		}
		
	}
}
