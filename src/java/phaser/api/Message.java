package phaser.api;

import com.lmax.disruptor.EventFactory;

public class Message {
    
    private Object payload;
    
    public static final EventFactory<Message> FACTORY = new EventFactory<Message>() {
        public Message newInstance() {
            return new Message();
        }
    };

    public Object getPayload() {
        return payload;
    }

    public void setPayload(final Object payload) {
        this.payload = payload;
    }        
}
