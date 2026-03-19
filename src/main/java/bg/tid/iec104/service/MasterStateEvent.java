package bg.tid.iec104.service;

import org.springframework.context.ApplicationEvent;

public class MasterStateEvent extends ApplicationEvent {
	
    private static final long serialVersionUID = 638012398239151114L;
    
	private final boolean isMaster;
    
    public MasterStateEvent(Object source, boolean isMaster) {
        super(source);
        this.isMaster = isMaster;
    }
    
    public boolean isMaster() {
        return isMaster;
    }
}
