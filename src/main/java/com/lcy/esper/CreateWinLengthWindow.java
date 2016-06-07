package com.lcy.esper;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;


class ContextPropertiesListener4 implements UpdateListener
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
 * 长度窗口案例，判断时间是否连续
 * @author lichangyue
 *
 */

public class CreateWinLengthWindow
{
	public static void main(String[] args) {
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
		EPAdministrator admin = epService.getEPAdministrator();
		EPRuntime runtime = epService.getEPRuntime();

		String esb = ESB.class.getName();
		
		/**
		  // FruitWindow保持最近10分钟的Apple事件  
			create window FruitWindow.win:time(10 min) as Apple 
			// context定义  
			create context esbtest2 partition by id from ESB  
  
			// 每当5个id相同的ESB事件进入时，统计price的总和  
			context esbtest select sum(price) from ESB.win:length_batch(5)  
  
			// 根据不同的id，统计3秒内进入的事件的平均price，且price必须大于10  
			context esbtest select avg(price) from ESB(price>10).win:time(3 sec)   
		 
		 */
		
		// 创建context
		String epl1 = "create context esbtest partition by id from " + esb +"(price >10)";
		// context.id针对不同的esb的id,price建立一个context，如果事件的id和price相同，则context.id也相同，即表明事件进入了同一个context
		String epl2 = "context esbtest select context.id,context.name,context.key1 ,price from " + esb +".win:length_batch(3)";
		
		admin.createEPL(epl1);
		EPStatement state = admin.createEPL(epl2);
		state.addListener(new ContextPropertiesListener4());

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
		
		ESB e22 = new ESB();
		e22.setId(2);
		e22.setPrice(30);
		System.out.println("sendEvent: id=2, price=30");
		runtime.sendEvent(e22);
		
		ESB e222 = new ESB();
		e222.setId(2);
		e222.setPrice(30);
		System.out.println("sendEvent: id=2, price=30");
		runtime.sendEvent(e222);
		
		ESB e6 = new ESB();
		e6.setId(1);
		e6.setPrice(35);
		System.out.println("sendEvent: id=1, price=35");
		runtime.sendEvent(e6);
	}
}