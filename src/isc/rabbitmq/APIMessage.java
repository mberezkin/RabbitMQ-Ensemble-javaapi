package isc.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.impl.LongStringHelper;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/// class APIMessage
public class APIMessage {
    public int MessageCount = 0;

    // BasicProperties
    public String ContentType = null;
    public String ContentEncoding = null;
    public String CorrelationId = null;
    public String ReplyTo = null;
    public String Expiration = null;
    public String MessageId = null;
    public String Type = null;
    public String UserId = null;
    public String AppId = null;
    public String ClusterId = null;
    public Integer DeliveryMode = null;
    public Integer Priority = null;
    public Date Timestamp = null;

    private byte[] _body;
    private final Map<String, Object> _headers;

    private final String _newLine;

    /// Constructor by default
    public APIMessage() throws Exception {
         _body = new byte[0];
        _headers = new HashMap<>();

        _newLine = System.lineSeparator();
    }

    /// Constructor with parameters: String contentType, int deliveryMode
    public APIMessage(String contentType, Integer deliveryMode) throws Exception {
        this(contentType, deliveryMode, null);
    }

    /// Constructor with parameters: String contentType, int deliveryMode, int priority
    public APIMessage(String contentType, Integer deliveryMode, Integer priority) throws Exception {
        this();

        if (contentType != null) ContentType = contentType;
        if (deliveryMode != null) DeliveryMode = deliveryMode;
        if (priority != null) Priority = priority;
    }

    public void clear() {
        MessageCount = 0;

        // BasicProperties
        ContentType = null;
        ContentEncoding = null;
        CorrelationId = null;
        ReplyTo = null;
        Expiration = null;
        MessageId = null;
        Type = null;
        UserId = null;
        AppId = null;
        ClusterId = null;
        DeliveryMode = null;
        Priority = null;
        Timestamp = null;

        _body = new byte[0];
        _headers.clear();
    }

    /// Return: int BodyLength
    public int getBodyLength() {
        return _body.length;
    }

    /// Return: String Body
    public String getBodyString() throws Exception {
        return new String(_body, StandardCharsets.UTF_8);
    }

    /// Parameters: String Body
    public void setBodyString(String bodyString) throws Exception {
        _body = (bodyString == null ? new byte[0] : bodyString.getBytes(StandardCharsets.UTF_8));
    }

    /// Return: byte[] Body
    public byte[] getBodyStream() throws Exception {
        return _body;
    }

    /// Parameters: byte[] Body
    public void setBodyStream(byte[] bodyStream) throws Exception {
        _body = (bodyStream == null ? new byte[0] : bodyStream);
    }

    public void clearHeaders() throws Exception {
        _headers.clear();
    }

    /// Parameters: String key, String value
    public void setHeader(String key, String value) throws Exception {
        _headers.put(key, LongStringHelper.asLongString(value));
    }

    /// Parameters: Map<String,Object>
    public void setHeaders(Map<String,Object> headers) throws Exception {
        _headers.clear();
        if (headers == null || headers.size() == 0) return;

        _headers.putAll(headers);
    }

    /// Parameters: String key
    /// Return: String value
    public String getHeader(String key) throws Exception {
        return _headers.get(key).toString();
    }

    /// Return: String array by format "key=value"
    public String[] getHeaders() throws Exception {
        if (_headers.size() == 0) return null;

        String[] headers = new String[_headers.size()];

        int i = 0;
        for (HashMap.Entry<String, Object> item : _headers.entrySet()) headers[i++] = item.getKey() + "=" + item.getValue();

        return headers;
    }

    /// Return: Map<String,String>
    public Map<String,String> getHeadersAsMap() throws Exception {
        Map<String,String> mapStrHeaders = new HashMap<>(_headers.size());
        for (Map.Entry<String, Object> item : _headers.entrySet()) mapStrHeaders.put(item.getKey(), item.getValue().toString());

        return mapStrHeaders;
    }

    /// Return: String rows by format "key=value"
    public String headersToString() throws Exception {
        if (_headers.size() == 0) return "";

        StringBuilder sbHeaders = new StringBuilder();
        for (String header : getHeaders()) sbHeaders.append(header).append(_newLine);

        return sbHeaders.toString();
    }

    // Return: AMQP.BasicProperties from fields of Message
    public AMQP.BasicProperties createProperties() throws Exception
    {
        return new AMQP.BasicProperties(ContentType, ContentEncoding, _headers, DeliveryMode, Priority, CorrelationId, ReplyTo, Expiration, MessageId, Timestamp, Type, UserId, AppId, ClusterId);
    }

    /// Return: String array by format "key=value"
    public String[] getProperties() throws Exception {
        List<String> lstProps = new ArrayList<>(15);

        lstProps.add("MessageCount=" + MessageCount);
        lstProps.add("BodyLength=" + getBodyLength());

        if (ContentType != null) lstProps.add("ContentType=" + ContentType);
        if (ContentEncoding != null) lstProps.add("ContentEncoding=" + ContentEncoding);
        if (CorrelationId != null) lstProps.add("CorrelationId=" + CorrelationId);
        if (ReplyTo != null) lstProps.add("ReplyTo=" + ReplyTo);
        if (Expiration != null) lstProps.add("Expiration=" + Expiration);
        if (MessageId != null) lstProps.add("MessageId=" + MessageId);
        if (Type != null) lstProps.add("Type=" + Type);
        if (UserId != null) lstProps.add("UserId=" + UserId);
        if (AppId != null) lstProps.add("AppId=" + AppId);
        if (ClusterId != null) lstProps.add("ClusterId=" + ClusterId);
        if (DeliveryMode != null) lstProps.add("DeliveryMode=" + DeliveryMode);
        if (Priority != null) lstProps.add("Priority=" + Priority);
        if (Timestamp != null) lstProps.add("Timestamp=" + Timestamp);

        return lstProps.toArray(new String[0]);
    }

    /// Return: String rows by format "key=value"
    public String propertiesToString() throws Exception {
        String [] props = getProperties();
        if ((props == null) || (props.length == 0)) return "";

        StringBuilder sbProps = new StringBuilder();
        for (String prop : props) sbProps.append(prop).append(_newLine);

        return sbProps.toString();
    }

    /// APIMessage to String
    @Override
    public String toString() {
        StringBuilder sbMessage = new StringBuilder();

        try {
            sbMessage.append("BasicProperties:").append(_newLine);
            sbMessage.append(propertiesToString()).append(_newLine);

            sbMessage.append("Headers:").append(_newLine);
            sbMessage.append(headersToString()).append(_newLine);

            sbMessage.append(getBodyString());
        } catch (Exception e) {
            sbMessage.append(e.getMessage()).append(_newLine);
        }

        return sbMessage.toString();
    }
}
