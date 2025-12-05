package queues

import (
	"encoding/xml"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// RFC1123Time Tests
// ============================================================================

func TestShouldMarshalTimeToRFC1123Format(t *testing.T) {
	// Arrange
	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	rfc1123Time := RFC1123Time(testTime)
	type Wrapper struct {
		XMLName xml.Name    `xml:"Wrapper"`
		Time    RFC1123Time `xml:"Time"`
	}
	wrapper := Wrapper{Time: rfc1123Time}

	// Act
	data, err := xml.Marshal(wrapper)

	// Assert
	require.NoError(t, err)
	assert.Contains(t, string(data), "Mon, 15 Jan 2024")
}

func TestShouldUnmarshalRFC1123TimeFromXML(t *testing.T) {
	// Arrange
	xmlData := `<Wrapper><Time>Mon, 15 Jan 2024 10:30:00 UTC</Time></Wrapper>`
	type Wrapper struct {
		XMLName xml.Name    `xml:"Wrapper"`
		Time    RFC1123Time `xml:"Time"`
	}

	// Act
	var wrapper Wrapper
	err := xml.Unmarshal([]byte(xmlData), &wrapper)

	// Assert
	require.NoError(t, err)
	expected := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	assert.Equal(t, expected, time.Time(wrapper.Time))
}

func TestShouldRoundTripRFC1123TimeThroughXML(t *testing.T) {
	// Arrange
	original := time.Date(2024, 6, 15, 14, 45, 30, 0, time.UTC)
	rfc1123Time := RFC1123Time(original)
	type Wrapper struct {
		XMLName xml.Name    `xml:"Wrapper"`
		Time    RFC1123Time `xml:"Time"`
	}
	wrapper := Wrapper{Time: rfc1123Time}

	// Act
	data, err := xml.Marshal(wrapper)
	require.NoError(t, err)
	var restored Wrapper
	err = xml.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, original.Unix(), time.Time(restored.Time).Unix())
}

// ============================================================================
// Message Serialization Tests
// ============================================================================

func TestShouldSerializeMessageToXML(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		MessageID:       "msg-123",
		InsertionTime:   RFC1123Time(now),
		ExpirationTime:  RFC1123Time(now.Add(7 * 24 * time.Hour)),
		PopReceipt:      "pop-456",
		TimeNextVisible: RFC1123Time(now.Add(30 * time.Second)),
		DequeueCount:    1,
		MessageText:     "SGVsbG8gV29ybGQ=",
	}

	// Act
	data, err := xml.Marshal(msg)
	require.NoError(t, err)
	var restored Message
	err = xml.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, msg.MessageID, restored.MessageID)
	assert.Equal(t, msg.PopReceipt, restored.PopReceipt)
	assert.Equal(t, msg.DequeueCount, restored.DequeueCount)
	assert.Equal(t, msg.MessageText, restored.MessageText)
}

func TestShouldHaveCorrectXMLElementNamesInMessage(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		MessageID:       "msg-123",
		InsertionTime:   RFC1123Time(now),
		ExpirationTime:  RFC1123Time(now.Add(time.Hour)),
		PopReceipt:      "pop-456",
		TimeNextVisible: RFC1123Time(now),
		DequeueCount:    0,
		MessageText:     "test message",
	}

	// Act
	data, err := xml.Marshal(msg)

	// Assert
	require.NoError(t, err)
	xmlStr := string(data)
	assert.Contains(t, xmlStr, "<MessageId>msg-123</MessageId>")
	assert.Contains(t, xmlStr, "<PopReceipt>pop-456</PopReceipt>")
	assert.Contains(t, xmlStr, "<DequeueCount>0</DequeueCount>")
	assert.Contains(t, xmlStr, "<MessageText>test message</MessageText>")
}

// ============================================================================
// Key Generation Tests
// ============================================================================

func TestShouldReturnCorrectQueueKey(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	key := h.queueKey("myqueue")

	// Assert
	assert.Equal(t, []byte("queues/meta/myqueue"), key)
}

func TestShouldReturnCorrectMessageKey(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	key := h.messageKey("myqueue", "msg-123")

	// Assert
	assert.Equal(t, []byte("queues/data/myqueue/msg-123"), key)
}

func TestShouldGenerateUniqueMessageIDs(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	id1 := h.generateMessageID()
	time.Sleep(time.Nanosecond)
	id2 := h.generateMessageID()

	// Assert
	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2)
}

func TestShouldGenerateUniquePopReceipts(t *testing.T) {
	// Arrange
	h := &Handler{}

	// Act
	pr1 := h.generatePopReceipt()
	time.Sleep(time.Nanosecond)
	pr2 := h.generatePopReceipt()

	// Assert
	assert.NotEmpty(t, pr1)
	assert.NotEmpty(t, pr2)
	assert.NotEqual(t, pr1, pr2)
}

// ============================================================================
// Message Visibility Tests
// ============================================================================

func TestShouldBeVisibleGivenTimeNextVisibleInPast(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		TimeNextVisible: RFC1123Time(now.Add(-time.Minute)),
	}

	// Act
	isVisible := !time.Time(msg.TimeNextVisible).After(now)

	// Assert
	assert.True(t, isVisible)
}

func TestShouldBeVisibleGivenTimeNextVisibleIsNow(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		TimeNextVisible: RFC1123Time(now),
	}

	// Act
	isVisible := !time.Time(msg.TimeNextVisible).After(now)

	// Assert
	assert.True(t, isVisible)
}

func TestShouldNotBeVisibleGivenTimeNextVisibleInFuture(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		TimeNextVisible: RFC1123Time(now.Add(time.Minute)),
	}

	// Act
	isVisible := !time.Time(msg.TimeNextVisible).After(now)

	// Assert
	assert.False(t, isVisible)
}

// ============================================================================
// Message Expiration Tests
// ============================================================================

func TestShouldBeExpiredGivenExpirationTimeInPast(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		ExpirationTime: RFC1123Time(now.Add(-time.Minute)),
	}

	// Act
	isExpired := time.Time(msg.ExpirationTime).Before(now)

	// Assert
	assert.True(t, isExpired)
}

func TestShouldNotBeExpiredGivenExpirationTimeIsNow(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		ExpirationTime: RFC1123Time(now),
	}

	// Act
	isExpired := time.Time(msg.ExpirationTime).Before(now)

	// Assert
	assert.False(t, isExpired)
}

func TestShouldNotBeExpiredGivenExpirationTimeInFuture(t *testing.T) {
	// Arrange
	now := time.Now().UTC()
	msg := Message{
		ExpirationTime: RFC1123Time(now.Add(time.Minute)),
	}

	// Act
	isExpired := time.Time(msg.ExpirationTime).Before(now)

	// Assert
	assert.False(t, isExpired)
}

// ============================================================================
// DequeueCount Tests
// ============================================================================

func TestShouldIncrementDequeueCountOnMultipleDequeues(t *testing.T) {
	// Arrange
	msg := Message{
		DequeueCount: 0,
	}

	// Act & Assert
	for i := 1; i <= 5; i++ {
		msg.DequeueCount++
		assert.Equal(t, i, msg.DequeueCount)
	}
}

// ============================================================================
// QueueMessagesList Tests
// ============================================================================

func TestShouldSerializeQueueMessagesListToXML(t *testing.T) {
	// Arrange
	type QueueMessagesList struct {
		XMLName      xml.Name  `xml:"QueueMessagesList"`
		QueueMessage []Message `xml:"QueueMessage"`
	}
	now := time.Now().UTC()
	messages := []Message{
		{
			MessageID:       "msg-1",
			InsertionTime:   RFC1123Time(now),
			ExpirationTime:  RFC1123Time(now.Add(time.Hour)),
			PopReceipt:      "pop-1",
			TimeNextVisible: RFC1123Time(now),
			DequeueCount:    0,
			MessageText:     "Message 1",
		},
		{
			MessageID:       "msg-2",
			InsertionTime:   RFC1123Time(now),
			ExpirationTime:  RFC1123Time(now.Add(time.Hour)),
			PopReceipt:      "pop-2",
			TimeNextVisible: RFC1123Time(now),
			DequeueCount:    1,
			MessageText:     "Message 2",
		},
	}
	list := QueueMessagesList{QueueMessage: messages}

	// Act
	data, err := xml.Marshal(list)

	// Assert
	require.NoError(t, err)
	xmlStr := string(data)
	assert.Contains(t, xmlStr, "<QueueMessagesList>")
	assert.Contains(t, xmlStr, "<QueueMessage>")
	assert.Contains(t, xmlStr, "msg-1")
	assert.Contains(t, xmlStr, "msg-2")
}

func TestShouldHandleEmptyQueueMessagesList(t *testing.T) {
	// Arrange
	type QueueMessagesList struct {
		XMLName      xml.Name  `xml:"QueueMessagesList"`
		QueueMessage []Message `xml:"QueueMessage"`
	}
	list := QueueMessagesList{QueueMessage: []Message{}}

	// Act
	data, err := xml.Marshal(list)
	require.NoError(t, err)
	var restored QueueMessagesList
	err = xml.Unmarshal(data, &restored)

	// Assert
	require.NoError(t, err)
	assert.Empty(t, restored.QueueMessage)
}
