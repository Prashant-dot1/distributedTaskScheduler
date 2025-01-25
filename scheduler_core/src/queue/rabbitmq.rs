use std::time::Duration;

use async_trait::async_trait;
use futures_lite::StreamExt;
use lapin::{options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, QueueDeclareOptions}, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties};

use crate::{error::SchedulerError, task::Task};

use super::MessageQueue;

const QUEUE_NAME: &str = "SCHEDULER_TASKS";

pub struct RabbitMQ {
    channel: Channel,
    quque_name : String
}

impl RabbitMQ {
    pub async fn new(url: &str) -> Result<Self, SchedulerError> {
        
        // need to create a connection
        let conn = Connection::connect(
            &url,
            ConnectionProperties::default(),
        ).await
        .map_err(|e| SchedulerError::QueueError(e.to_string()))?;


        // CREATE A CHANNEL
        let channel = conn.create_channel()
                    .await.map_err(|e| SchedulerError::QueueError(e.to_string()))?;

        
        // we declare a queue in the channel
        let queue = channel.queue_declare(
            QUEUE_NAME,
            QueueDeclareOptions {
                durable:true,
                auto_delete: false,
                ..Default::default()
            },
            FieldTable::default()
        ).await
        .map_err(|e| SchedulerError::QueueError(e.to_string()))?;
        

        // basic Qos 
        channel.basic_qos(1, BasicQosOptions::default())
        .await
        .map_err(|e| SchedulerError::QueueError(e.to_string()))?;


        Ok(Self { channel: channel, quque_name: QUEUE_NAME.to_string() })
    }
}

#[async_trait]
impl MessageQueue for RabbitMQ {

    async fn publish_task(&self, task: Task) -> Result<(), SchedulerError> {
        
        //item to be set inside of the queue
        let payload = serde_json::to_vec(&task)
        .map_err(|e| SchedulerError::QueueError(format!("serialization of the task failed {}", e)))?;

        // publish 
        self.channel.basic_publish( 
            "", 
            &self.quque_name,
            BasicPublishOptions {
                mandatory: true,
                ..Default::default()
            },
            &payload, 
            BasicProperties::default()
        ).await
        .map_err(|e| SchedulerError::QueueError(e.to_string()))?;
        
        Ok(())
    }

    async fn consume_task(&self) -> Result<Option<Task>, SchedulerError> {
        
        let mut consumer = self.channel.basic_consume(
            &self.quque_name,
            "",
            BasicConsumeOptions {
                no_ack : false,
                ..Default::default()
            }, 
            FieldTable::default()
        ).await
        .map_err(|e| SchedulerError::QueueError(e.to_string()))?;

        // consume from the queue after 1 sec interval
        if let Ok(Some(delivery)) = tokio::time::timeout(Duration::from_secs(1), consumer.next()).await {
             
            let delivery = delivery.map_err(|e| SchedulerError::QueueError(e.to_string()))?;
            let task = serde_json::from_slice::<Task>(&delivery.data)
                                .map_err(|e| SchedulerError::QueueError(e.to_string()))?;
            
            // ACK
            delivery.ack(BasicAckOptions::default())
            .await
            .map_err(|e| SchedulerError::QueueError(e.to_string()))?;

            Ok(Some(task))
        }
        else {
            Ok(None)
        }
    }
}