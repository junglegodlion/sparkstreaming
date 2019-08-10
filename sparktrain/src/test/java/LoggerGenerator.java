import org.apache.log4j.Logger;

/**
 * 模拟日志产生
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws Exception{

        int index = 0;
        while(true) {
            //启动一个线程，休息一下
            Thread.sleep(1000);
            logger.info("value : " + index++);
        }
    }
}
