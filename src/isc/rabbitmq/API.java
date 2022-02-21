package isc.rabbitmq;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.LongStringHelper;

import java.io.IOException;

import java.nio.charset.StandardCharsets;
//import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
//import java.util.List;

/**
 * Created by eduard on 06.10.2017.
 * Edited by mberezkin on 21.02.2022
 */
public class API {
    private com.rabbitmq.client.Channel _channel;

    private final String _queue;

    private final String _exchange;

    private final Connection _connection;

    // add all props except CorrelationId, MessageId, Headers which sets from parameters of functions
    public String ContentType = null;
    public String ContentEncoding = null;
    public Integer DeliveryMode = null;
    public Integer Priority = null;
    public String ReplyTo = null;
    public String Expiration = null;
    public Date Timestamp = null;
    public String Type = null;
    public String UserId = null;
    public String AppId = null;
    public String ClusterId = null;

    // class for information about last error
    private static class LastError {

        // add flag last error and last error message
        private static boolean _lastError = false;
        private static String _lastErrorMessage = "";
        private static String _lastErrorHeader = "";

        // functions for using information about last error
        public static void Clear() {
            _lastError = false;
            _lastErrorMessage = "";
            _lastErrorHeader = "";
        }

        public static void SetHeader(String errorHeader) {
            _lastErrorHeader = (errorHeader == null ? "" : errorHeader);
        }

        public static void SetMessage(String errorMessage) {
            SetMessage(errorMessage, null);
        }

        public static void SetMessage(String errorMessage, String errorHeader) {
            _lastError = true;

            String Header = ((errorHeader == null) || (errorHeader.isEmpty()) ? _lastErrorHeader : errorHeader);
            _lastErrorMessage = "["+ Header +"]: " + ((errorMessage == null) || (errorMessage.isEmpty()) ? "Undefined error" : errorMessage);
        }

        public static boolean IsError() {
            return _lastError;
        }

        public static String GetMessage() {
            return "ERROR "+_lastErrorMessage;
        }
    }

    public boolean isLastError() {
        return LastError.IsError();
    }

    public String getLastErrorMessage() {
        return LastError.GetMessage();
    }

    public API(String host, int port, String user, String pass, String virtualHost, String queue)  throws Exception {
        this(host, port, user, pass, virtualHost, queue, null);
    }

    public API(String host, int port, String user, String pass, String virtualHost, String queue, String exchange)  throws Exception {
        _queue = queue;

        Connection con = null;
        try {

            ConnectionFactory factory = new ConnectionFactory();

            if (host.toLowerCase().startsWith("amqp://")) {
                // we got URI connection string
                factory.setUri(host);
            } else {
                factory.setHost(host);
                factory.setPort(port);
                factory.setUsername(user);
                factory.setPassword(pass);
                factory.setVirtualHost(virtualHost);
            }

            //factory.setAutomaticRecoveryEnabled(true);
            factory.setRequestedHeartbeat(0);

            LastError.SetHeader("API::newConnection");
            con = factory.newConnection();

            LastError.SetHeader("API::createChannel");
            _channel = con.createChannel();

            // Do we need to declare queue?
            // No if we're sending by exchange/routing_key
            if (exchange != null && !exchange.isEmpty()) {
                // Check that queue exists
                // Method throws exception if queue does not exist or is exclusive
                // Correct exception text: channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND - no queue 'queue'
                LastError.SetHeader("API::queueDeclarePassive");
                _channel.queueDeclarePassive(queue); // AMQP.Queue.DeclareOk
            }

            // if connect to queue successful continue
            if (exchange != null) {
                LastError.SetHeader("API::exchangeDeclarePassive");
                _channel.exchangeDeclarePassive(exchange); // AMQP.Exchange.DeclareOk
            }

        } catch (java.io.IOException ex) {
            LastError.SetMessage(ex.getMessage());
        }

        _connection = con;
        _exchange = (exchange != null ? exchange : "");
    }

    // durable - true if we are declaring a durable queue (the queue will survive a server restart)
    // exclusive - true if we are declaring an exclusive queue (restricted to this connection)
    // autoDelete - true if we are declaring an autodelete queue (server will delete it when no longer in use)
    // arguments - other properties (construction arguments) for the queue
    public boolean queueDeclare(Boolean durable, Boolean exclusive, Boolean autoDelete) throws Exception {
        LastError.Clear();

        try {
            LastError.SetHeader("API::createChannel");
            if (!_channel.isOpen()) _channel = _connection.createChannel();

            LastError.SetHeader("API::queueDeclare");
            _channel.queueDeclare(_queue, durable, exclusive, autoDelete, null);
        } catch (Exception ex) {
            LastError.SetMessage(ex.getMessage());
        }

        return LastError.IsError();
    }

    // type - direct, topic, fanout, headers. See https://lostechies.com/derekgreer/2012/03/28/rabbitmq-for-windows-exchange-types/
    // passive - if true, works the same as exchangeDeclarePassive
    // durable - true if we are declaring a durable exchange (the exchange will survive a server restart)
    // autoDelete - true if we are declaring an autodelete exchange (server will delete it when no longer in use)
    // arguments - other properties (construction arguments) for the exchange
    public boolean exchangeDeclare(String type, Boolean passive, Boolean durable, Boolean autoDelete) throws Exception {
        LastError.Clear();

        try {
            LastError.SetHeader("API::createChannel");
            if (!_channel.isOpen()) _channel = _connection.createChannel();

            LastError.SetHeader("API::exchangeDeclare");
            _channel.exchangeDeclare(_exchange, type, passive, durable, autoDelete, null); // AMQP.Exchange.DeclareOk
        } catch (Exception ex) {
            LastError.SetMessage(ex.getMessage());
        }

        return LastError.IsError();
    }

    public void sendMessageId(byte[] msg, String correlationId, String messageId) throws Exception {
        sendMessageToQueueId(_queue, msg, correlationId, messageId);
    }

    public void sendMessage(byte[] msg) throws Exception {
        sendMessageToQueue(_queue, msg);
    }

    public void sendMessageToQueue(String queue, byte[] msg) throws Exception {
        sendMessageToQueueId(queue, msg, null, null);
    }

    public void sendMessageToQueueId(String queue, byte[] msg, String correlationId, String messageId) throws Exception {
        LastError.Clear();

        try {
            LastError.SetHeader("API::sendMessageToQueueId");
            AMQP.BasicProperties props = createProperties(correlationId, messageId);
            _channel.basicPublish(_exchange, queue, props, msg);
        } catch (java.io.IOException ex) {
            LastError.SetMessage(ex.getMessage());
        }
    }

    // send message with headers
    public void sendMessageIdHeaders(byte[] msg, String correlationId, String messageId, String[] arHeaders) throws Exception {
        sendMessageToQueueIdHeaders(_queue, msg, correlationId, messageId, arHeaders);
    }

    // send message to queue with headers
    public void sendMessageToQueueIdHeaders(String queue, byte[] msg, String correlationId, String messageId, String[] arHeaders) throws Exception {
        LastError.Clear();
        try {
            LastError.SetHeader("API::createProperties");
            AMQP.BasicProperties props = createProperties(correlationId, messageId, arHeaders);

            LastError.SetHeader("API::sendMessageToQueueIdHeaders");
            _channel.basicPublish(_exchange, queue, props, msg);
        } catch ( Exception ex) {
            LastError.SetMessage(ex.getMessage());
        }
    }

    public byte[] readMessageStream(String[] msg) throws Exception {
        return readMessageStream(msg, null);
    }

    public byte[] readMessageStream(String[] msg, String[] headers) throws Exception {
        GetResponse response = readMessage(msg, headers);
        if (response == null) return new byte[0];

        return response.getBody();
    }

    public String[] readMessageString() throws Exception {
        return readMessageString(null);
    }

    public String[] readMessageString(String[] headers) throws Exception {
        String[] msg = new String[16];

        GetResponse response = readMessage(msg, headers);
        if (response != null) msg[15] = new String(response.getBody(), StandardCharsets.UTF_8);

        return msg;
    }

    public String readMessageBodyString() throws Exception {
        String result = "";
        boolean autoAck = true;

        LastError.Clear();
        try {
            LastError.SetHeader("API::readMessageBodyString");
            GetResponse response = _channel.basicGet(_queue, autoAck);

            if (response != null) {
               result = new String(response.getBody(), StandardCharsets.UTF_8);
            }
        } catch (Exception ex) {
            LastError.SetMessage(ex.getMessage());
        }

        return result;
    }

    // Get message and fill basic array props
    private GetResponse readMessage(String[] msg) throws Exception {
        return readMessage(msg, null);
    }

    // Get message and fill basic array props and Headers (string array by format "key=value")
    private GetResponse readMessage(String[] msg, String[] headers) throws Exception {
        boolean autoAck = true;
        GetResponse response = null;

        LastError.Clear();
        try {
            LastError.SetHeader("API::readMessage");
            response = _channel.basicGet(_queue, autoAck);

            if (response == null) {
                // No message retrieved.
                response = new GetResponse(null, null, new byte[0], 0);
            } else {
                AMQP.BasicProperties props = response.getProps();
                msg[0] =  Integer.toString(response.getBody().length);
                msg[1] =  Integer.toString(response.getMessageCount());
                msg[2] = props.getContentType();
                msg[3] = props.getContentEncoding();
                msg[4] = props.getCorrelationId();
                msg[5] = props.getReplyTo();
                msg[6] = props.getExpiration();
                msg[7] = props.getMessageId();
                msg[8] = props.getType();
                msg[9] = props.getUserId();
                msg[10] = props.getAppId();
                msg[11] = props.getClusterId();
                msg[12] = props.getDeliveryMode() != null ? Integer.toString(props.getDeliveryMode()) : null;
                msg[13] = props.getPriority() != null ? Integer.toString(props.getPriority()) : null;
                msg[14] = props.getTimestamp() != null ? props.getTimestamp().toString() : null;

                // fill headers
                if (headers != null && headers.length > 0) {
                    Map<String, Object> mapHeaders = props.getHeaders();
                    if (mapHeaders != null && mapHeaders.size() > 0) {
                        int i = 0;
                        for (HashMap.Entry<String, Object> item : mapHeaders.entrySet()) {
                            if (i == headers.length) break;
                            headers[i++] = item.getKey() + "=" + item.getValue();
                        }
                    }
                }
            }
        } catch ( Exception ex) {
            LastError.SetMessage(ex.getMessage());
        }

        return response;
    }

    public Boolean isOpen()
    {
        boolean result = false;

        LastError.Clear();
        try {
            LastError.SetHeader("API::isOpen");
            result = _connection != null && _connection.isOpen();
        } catch ( Exception ex) {
            LastError.SetMessage(ex.getMessage());
        }

        return result;
    }

    public void close()throws Exception {
        try {
            _channel.close();
        } catch ( Exception ex) {}

        try {
            _connection.close();
        } catch ( Exception ex) {}
    }

    // add parameter arHeaders (null by default for compatibility with previous version)
    private AMQP.BasicProperties createProperties(String correlationId, String messageId) throws Exception
    {
        return createProperties(correlationId, messageId, null);
    }

    // add parameter arHeaders (string array by format "key=value")
    private AMQP.BasicProperties createProperties(String correlationId, String messageId, String[] arHeaders) throws Exception
    {
        String contentType = ContentType;
        String contentEncoding = ContentEncoding;

        // fill headers from string array by format "key=value"
        HashMap<String, Object> headers = new HashMap<>();
        if ((arHeaders != null) && (arHeaders.length > 0)) {
            for (int i = 0; i < arHeaders.length; i++) {
                String[] subStr = arHeaders[i].split("=");
                if (subStr.length == 2) headers.put(subStr[0], LongStringHelper.asLongString(subStr[1]));
            }
        }

        Integer deliveryMode = DeliveryMode;
        Integer priority = Priority;
        //String correlationId = null;
        String replyTo = ReplyTo;
        String expiration = Expiration;
        //String messageId= null;
        Date timestamp = Timestamp == null ? new Date() : Timestamp;
        String type = Type;
        String userId= UserId;
        String appId = AppId;
        String clusterId= ClusterId;

        return new AMQP.BasicProperties(contentType, contentEncoding, headers, deliveryMode, priority, correlationId, replyTo, expiration, messageId, timestamp, type, userId, appId, clusterId);
    }

}
