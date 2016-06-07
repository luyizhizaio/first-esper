package com.lcy.esper;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

class ESB2
{

	private int id;
	private int price;

	public int getId()
	{
		return id;
	}

	public void setId(int id)
	{
		this.id = id;
	}

	public int getPrice()
	{
		return price;
	}

	public void setPrice(int price)
	{
		this.price = price;
	}

}

class ContextPropertiesListener3 implements UpdateListener
{

	public void update(EventBean[] newEvents, EventBean[] oldEvents)
	{
		if (newEvents != null)
		{
			EventBean event = newEvents[0];
			System.out.println("context.name " + event.get("name") + ", context.id " + event.get("id") + ", context.key1 " + event.get("key1")
					);
		}
	}
}
/**
 * 长度窗口案例
 * @author lichangyue
 *
 */

public class LengthWindow
{
	public static void main(String[] args) {
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
		EPAdministrator admin = epService.getEPAdministrator();
		EPRuntime runtime = epService.getEPRuntime();

		String esb = ESB.class.getName();
		// 创建context
		String epl1 = "create context esbtest partition by id from " + esb;
		/**
		 create context esbtest partition by url, ip from  ngnix_log
		 insert into ngixsum  as select url ,ip count(1) from ngnix_log.win:time(1 min)
		 
		 */
		// 长度为2出发，
		String epl2 = "context esbtest select context.id,context.name,context.key1 ,price from " + esb+".win:length(2)";

		admin.createEPL(epl1);
		EPStatement state = admin.createEPL(epl2);
		state.addListener(new ContextPropertiesListener3());

		ESB e1 = new ESB();
		e1.setId(1);
		e1.setPrice(20);
		System.out.println("sendEvent: id=1, price=20");
		runtime.sendEvent(e1);


		ESB e2 = new ESB();
		e2.setId(2);
		e2.setPrice(30);
		System.out.println("sendEvent: id=2, price=30");
		runtime.sendEvent(e2);

		ESB e3 = new ESB();
		e3.setId(1);
		e3.setPrice(30);
		System.out.println("sendEvent: id=1, price=30");
		runtime.sendEvent(e3);

		ESB e4 = new ESB();
		e4.setId(4);
		e4.setPrice(20);
		System.out.println("sendEvent: id=4, price=20");
		runtime.sendEvent(e4);
		
		
		ESB e5 = new ESB();
		e5.setId(1);
		e5.setPrice(35);
		System.out.println("sendEvent: id=1, price=35");
		runtime.sendEvent(e5);
		
		
		ESB e6 = new ESB();
		e6.setId(1);
		e6.setPrice(35);
		System.out.println("sendEvent: id=1, price=35");
		runtime.sendEvent(e6);
	}
}