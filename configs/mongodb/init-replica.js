// MongoDB Replica Set Initialization Script
// This script initializes a MongoDB replica set for the event processing pipeline

try {
    // Check if replica set is already initialized
    var config = rs.config();
    print("Replica set already initialized");
} catch (err) {
    print("Initializing replica set...");
    
    // Initialize replica set configuration
    var config = {
        _id: "rs0",
        members: [
            {
                _id: 0,
                host: "mongodb-1:27017",
                priority: 2
            },
            {
                _id: 1,
                host: "mongodb-2:27017",
                priority: 1
            },
            {
                _id: 2,
                host: "mongodb-3:27017",
                priority: 1
            }
        ]
    };
    
    // Initiate the replica set
    rs.initiate(config);
    
    print("Replica set initialization started...");
    print("Waiting for replica set to be ready...");
    
    // Wait for primary to be elected
    while (!rs.isMaster().ismaster) {
        sleep(1000);
    }
    
    print("Replica set is ready!");
    
    // Switch to events database
    db = db.getSiblingDB('events');
    
    // Create application user
    db.createUser({
        user: "events_user",
        pwd: "events_pass",
        roles: [
            {
                role: "readWrite",
                db: "events"
            },
            {
                role: "dbAdmin",
                db: "events"
            }
        ]
    });
    
    print("Application user created successfully");
    
    // Create collections with validation
    db.createCollection("raw_events", {
        validator: {
            $jsonSchema: {
                bsonType: "object",
                required: ["event_id", "user_id", "event_type", "timestamp"],
                properties: {
                    event_id: {
                        bsonType: "string",
                        description: "Unique event identifier"
                    },
                    user_id: {
                        bsonType: "int",
                        description: "User identifier"
                    },
                    event_type: {
                        bsonType: "string",
                        description: "Type of event"
                    },
                    timestamp: {
                        bsonType: "string",
                        description: "Event timestamp"
                    }
                }
            }
        }
    });
    
    print("Raw events collection created with validation");
    
    // Create indexes for better performance
    db.raw_events.createIndex({ "event_id": 1 }, { unique: true });
    db.raw_events.createIndex({ "user_id": 1, "timestamp": -1 });
    db.raw_events.createIndex({ "event_type": 1, "timestamp": -1 });
    db.raw_events.createIndex({ "session_id": 1 });
    db.raw_events.createIndex({ "timestamp": -1 });
    
    print("Indexes created successfully");
    
    // Create capped collection for real-time monitoring
    db.createCollection("event_stats", {
        capped: true,
        size: 1048576, // 1MB
        max: 1000
    });
    
    print("Event stats collection created");
    
    print("MongoDB initialization completed successfully!");
    
} catch (error) {
    print("Error during initialization: " + error);
    throw error;
}