package isc.rabbitmq;

import com.rabbitmq.client.*;

/**
 * Created by eduard on 06.10.2017.
 *  Edited by mberezkin on 21.02.2022
 */

public class API {
    private com.rabbitmq.client.Channel _channel;

    private final String _queue;

    private final String _exchange;

    private final Connection _connection;

    // class for information about last error
    private static class LastError {

        // add flag last error and last error message
        private static boolean _lastError = false;
        private static String _lastErrorMessage = "";
        private static String _lastErrorHeader = "";

        // functions for using information about last error
        public static void clear() {
            _lastError = false;
            _lastErrorMessage = "";
            _lastErrorHeader = "";
        }

        public static void setHeader(String errorHeader) {
            _lastErrorHeader = (errorHeader == null ? "" : errorHeader);
        }

        public static void setMessage(String errorMessage) {
            setMessage(errorMessage, null);
        }

        public static void setMessage(String errorMessage, String errorHeader) {
            _lastError = true;

            String Header = ((errorHeader == null) || (errorHeader.isEmpty()) ? _lastErrorHeader : errorHeader);
            _lastErrorMessage = "["+ Header +"]: " + ((errorMessage == null) || (errorMessage.isEmpty()) ? "Undefined error" : errorMessage);
        }

        public static boolean isError() {
            return _lastError;
        }

        public static String getMessage() {
            return "ERROR "+_lastErrorMessage;
        }
    }

    public boolean isLastError() {
        return LastError.isError();
    }

    public String getLastErrorMessage() {
        return LastError.getMessage();
    }

    /// String host, int port, String user, String pass, String virtualHost, String queue
    public API(String host, int port, String user, String pass, String virtualHost, String queue)  throws Exception {
        this(host, port, user, pass, virtualHost, queue, null);
    }

    /// String host, int port, String user, String pass, String virtualHost, String queue, String exchange
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

            LastError.setHeader("API::newConnection");
            con = factory.newConnection();

            LastError.setHeader("API::createChannel");
            _channel = con.createChannel();

            // Do we need to declare queue?
            // No if we're sending by exchange/routing_key
            if (exchange != null && !exchange.isEmpty()) {
                // Check that queue exists
                // Method throws exception if queue does not exist or is exclusive
                // Correct exception text: channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND - no queue 'queue'
                LastError.setHeader("API::queueDeclarePassive");
                _channel.queueDeclarePassive(queue); // AMQP.Queue.DeclareOk
            }

            // if connect to queue successful continue
            if (exchange != null) {
                LastError.setHeader("API::exchangeDeclarePassive");
                _channel.exchangeDeclarePassive(exchange); // AMQP.Exchange.DeclareOk
            }

        } catch (java.io.IOException ex) {
            LastError.setMessage(ex.getMessage());
        }

        _connection = con;
        _exchange = (exchange != null ? exchange : "");
    }

    /// durable - true if we are declaring a durable queue (the queue will survive a server restart)
    /// exclusive - true if we are declaring an exclusive queue (restricted to this connection)
    /// autoDelete - true if we are declaring an autodelete queue (server will delete it when no longer in use)
    /// arguments - other properties (construction arguments) for the queue
    public boolean queueDeclare(Boolean durable, Boolean exclusive, Boolean autoDelete) throws Exception {
        LastError.clear();

        try {
            LastError.setHeader("API::createChannel");
            if (!_channel.isOpen()) _channel = _connection.createChannel();

            LastError.setHeader("API::queueDeclare");
            _channel.queueDeclare(_queue, durable, exclusive, autoDelete, null);
        } catch (Exception ex) {
            LastError.setMessage(ex.getMessage());
        }

        return LastError.isError();
    }

    /// type - direct, topic, fanout, headers. See https://lostechies.com/derekgreer/2012/03/28/rabbitmq-for-windows-exchange-types/
    /// passive - if true, works the same as exchangeDeclarePassive
    /// durable - true if we are declaring a durable exchange (the exchange will survive a server restart)
    /// autoDelete - true if we are declaring an autodelete exchange (server will delete it when no longer in use)
    /// arguments - other properties (construction arguments) for the exchange
    public boolean exchangeDeclare(String type, Boolean passive, Boolean durable, Boolean autoDelete) throws Exception {
        LastError.clear();
        try {
            LastError.setHeader("API::createChannel");
            if (!_channel.isOpen()) _channel = _connection.createChannel();

            LastError.setHeader("API::exchangeDeclare");
            _channel.exchangeDeclare(_exchange, type, passive, durable, autoDelete, null); // AMQP.Exchange.DeclareOk
        } catch (Exception ex) {
            LastError.setMessage(ex.getMessage());
        }

        return LastError.isError();
    }

    /// Parameters: APIMessage message
    public void sendMessage(APIMessage message) throws Exception {
        sendMessageToQueue(_queue, message);
    }

    /// Parameters: String queue, APIMessage message
    public void sendMessageToQueue(String queue, APIMessage message) throws Exception {
        LastError.clear();
        try {
            LastError.setHeader("API::createProperties");
            AMQP.BasicProperties props = message.createProperties();

            LastError.setHeader("API::sendMessageToQueue");
            _channel.basicPublish(_exchange, queue, props, message.getBodyStream());
        } catch ( Exception ex) {
            LastError.setMessage(ex.getMessage());
        }
    }

    /// Return: APIMessage
    public APIMessage readMessage() throws Exception {
        return readMessageFromQueue(_queue);
    }

    /// Parameters: String queue
    /// Return: APIMessage
    public APIMessage readMessageFromQueue(String queue) throws Exception {
        boolean autoAck = true;
        APIMessage message = new APIMessage();

        LastError.clear();
        try {
            LastError.setHeader("API::getMessageFromQueue");
            GetResponse response = _channel.basicGet(queue, autoAck);
            if (response == null) return message;

            LastError.setHeader("API::getProps");
            AMQP.BasicProperties props = response.getProps();

            message.MessageCount = response.getMessageCount();

            // BasicProperties
            message.ContentType = props.getContentType();
            message.ContentEncoding = props.getContentEncoding();
            message.CorrelationId = props.getCorrelationId();
            message.ReplyTo = props.getReplyTo();
            message.Expiration = props.getExpiration();
            message.MessageId = props.getMessageId();
            message.Type = props.getType();
            message.UserId = props.getUserId();
            message.AppId = props.getAppId();
            message.ClusterId = props.getClusterId();
            message.DeliveryMode = props.getDeliveryMode();
            message.Priority = props.getPriority() ;
            message.Timestamp = props.getTimestamp();

            // fill headers
            LastError.setHeader("API::getMessageFromQueue.setHeaders");
            message.setHeaders(props.getHeaders());

            // Body
            message.setBodyStream(response.getBody());
        } catch ( Exception ex) {
            LastError.setMessage(ex.getMessage());
        }

        return message;
    }

    public Boolean isOpen()
    {
        boolean result = false;

        LastError.clear();
        try {
            LastError.setHeader("API::isOpen");
            result = _connection != null && _connection.isOpen();
        } catch ( Exception ex) {
            LastError.setMessage(ex.getMessage());
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

}
