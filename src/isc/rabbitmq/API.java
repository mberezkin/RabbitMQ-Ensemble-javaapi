package isc.rabbitmq;

import com.rabbitmq.client.*;
import java.util.Date;

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
    private class LastError {

        // add flag last error and last error message
        private boolean _lastError = false;
        private String _lastErrorMessage = "";
        private String _lastErrorHeader = "";

        // functions for using information about last error
        public void clear() {
            _lastError = false;
            _lastErrorMessage = "";
            _lastErrorHeader = "";
        }

        public void setHeader(String errorHeader) {
            _lastErrorHeader = (errorHeader == null ? "" : errorHeader);
        }

        public void setMessage(String errorMessage) {
            setMessage(errorMessage, null);
        }

        public void setMessage(String errorMessage, String errorHeader) {
            _lastError = true;

            String Header = ((errorHeader == null) || (errorHeader.isEmpty()) ? _lastErrorHeader : errorHeader);
            _lastErrorMessage = "["+ Header +"]: " + ((errorMessage == null) || (errorMessage.isEmpty()) ? "Undefined error" : errorMessage);
        }

        public boolean isError() {
            return _lastError;
        }

        public String getMessage() {
            return "ERROR "+_lastErrorMessage;
        }
    }

    private final LastError _lastError;

    public boolean isLastError() {
        return _lastError.isError();
    }

    public String getLastErrorMessage() {
        return _lastError.getMessage();
    }

    /// String host, int port, String user, String pass, String virtualHost, String queue
    public API(String host, int port, String user, String pass, String virtualHost, String queue)  throws Exception {
        this(host, port, user, pass, virtualHost, queue, null);
    }

    /// String host, int port, String user, String pass, String virtualHost, String queue, String exchange
    public API(String host, int port, String user, String pass, String virtualHost, String queue, String exchange)  throws Exception {
        _lastError = new LastError();
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

            _lastError.setHeader("API::newConnection");
            con = factory.newConnection();

            _lastError.setHeader("API::createChannel");
            _channel = con.createChannel();

            if (queue != null && !queue.isEmpty()) {
                // Check that queue exists
                // Method throws exception if queue does not exist or is exclusive
                // Correct exception text: channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND - no queue 'queue'
                _lastError.setHeader("API::queueDeclarePassive");
                _channel.queueDeclarePassive(queue); // AMQP.Queue.DeclareOk
            }

            if (exchange != null && !exchange.isEmpty()) {
                // Check that exchange exists
                _lastError.setHeader("API::exchangeDeclarePassive");
                _channel.exchangeDeclarePassive(exchange); // AMQP.Exchange.DeclareOk
            }

        } catch (java.io.IOException ex) {
            _lastError.setMessage(ex.getMessage());
        }

        _connection = con;
        _queue = (queue != null ? queue : "");
        _exchange = (exchange != null ? exchange : "");
    }

    /// durable - true if we are declaring a durable queue (the queue will survive a server restart)
    /// exclusive - true if we are declaring an exclusive queue (restricted to this connection)
    /// autoDelete - true if we are declaring an autodelete queue (server will delete it when no longer in use)
    /// arguments - other properties (construction arguments) for the queue
    public boolean queueDeclare(Boolean durable, Boolean exclusive, Boolean autoDelete) throws Exception {
        _lastError.clear();

        try {
            _lastError.setHeader("API::createChannel");
            if (!_channel.isOpen()) _channel = _connection.createChannel();

            _lastError.setHeader("API::queueDeclare");
            _channel.queueDeclare(_queue, durable, exclusive, autoDelete, null);
        } catch (Exception ex) {
            _lastError.setMessage(ex.getMessage());
        }

        return _lastError.isError();
    }

    /// type - direct, topic, fanout, headers. See https://lostechies.com/derekgreer/2012/03/28/rabbitmq-for-windows-exchange-types/
    /// passive - if true, works the same as exchangeDeclarePassive
    /// durable - true if we are declaring a durable exchange (the exchange will survive a server restart)
    /// autoDelete - true if we are declaring an autodelete exchange (server will delete it when no longer in use)
    /// arguments - other properties (construction arguments) for the exchange
    public boolean exchangeDeclare(String type, Boolean passive, Boolean durable, Boolean autoDelete) throws Exception {
        _lastError.clear();
        try {
            _lastError.setHeader("API::createChannel");
            if (!_channel.isOpen()) _channel = _connection.createChannel();

            _lastError.setHeader("API::exchangeDeclare");
            _channel.exchangeDeclare(_exchange, type, passive, durable, autoDelete, null); // AMQP.Exchange.DeclareOk
        } catch (Exception ex) {
            _lastError.setMessage(ex.getMessage());
        }

        return _lastError.isError();
    }

    /// Parameters: APIMessage message
    public void sendMessage(APIMessage message) throws Exception {
        sendMessageToQueue(_queue, message);
    }

    /// Parameters: String queue, APIMessage message
    public void sendMessageToQueue(String queue, APIMessage message) throws Exception {
        _lastError.clear();
        try {
            _lastError.setHeader("API::createProperties");
            AMQP.BasicProperties props = message.createProperties();

            _lastError.setHeader("API::sendMessageToQueue");
            _channel.basicPublish(_exchange, queue, props, message.getBodyStream());
        } catch ( Exception ex) {
            _lastError.setMessage(ex.getMessage());
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

        _lastError.clear();
        try {
            _lastError.setHeader("API::getMessageFromQueue");
            GetResponse response = _channel.basicGet(queue, autoAck);
            if (response == null) return message;

            _lastError.setHeader("API::getProps");
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

            Date timestamp = props.getTimestamp();
            if (timestamp != null) message.setTimestampMilliSeconds(timestamp.getTime());
            else message.setTimestamp(null);

            // fill headers
            _lastError.setHeader("API::getMessageFromQueue.setHeaders");
            message.setHeaders(props.getHeaders());

            // Body
            message.setBodyStream(response.getBody());
        } catch ( Exception ex) {
            _lastError.setMessage(ex.getMessage());
        }

        return message;
    }

    public Boolean isOpen()
    {
        boolean result = false;

        _lastError.clear();
        try {
            _lastError.setHeader("API::isOpen");
            result = _connection != null && _connection.isOpen();
        } catch ( Exception ex) {
            _lastError.setMessage(ex.getMessage());
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
