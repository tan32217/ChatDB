import { Component } from '@angular/core';
import { ChatService } from './services/chatservice';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'ChatDB';
  
  public chatService: ChatService; // Ensure chatService is public

  constructor(chatService: ChatService) {
    this.chatService = chatService;
  }

  public fetchMetadata() {
    console.log('Fetch Metadata button clicked'); // Add debug log
    this.chatService.fetchMongoDBMetadata().subscribe(
      (metadata) => {
        console.log('Metadata received:', metadata); // Debug log
        this.chatService.addMessage({
          sender: 'db',
          content: JSON.stringify(metadata)
        });
      },
      (error) => {
        console.error('Error fetching metadata:', error); // Debug log
      }
    );
  }
  // Handle queries for both MySQL and MongoDB
  handleUserQuery(event: { query: string; type: 'mysql' | 'mongodb' }) {
    const { query, type } = event;

    // Display the user's query in the chat window
    this.chatService.addMessage({
      sender: 'user',
      content: query
    });

    if (type === 'mysql') {
      // Handle MySQL Query
      this.chatService.convertNLQToSQL(query).subscribe(
        (response: any) => {
          const sqlQuery = response.sql; // Assume the backend returns { sql: "SQL QUERY" }

          // Display the generated SQL query
          this.chatService.addMessage({
            sender: 'db',
            content: `Generated SQL query: ${sqlQuery}`
          });

          // Execute the SQL query
          this.chatService.sendMessageToSQL({ query: sqlQuery });
        },
        (error) => {
          // Display error
          this.chatService.addMessage({
            sender: 'db',
            content: `Error generating SQL query: ${error.message}`
          });
        }
      );
    } else if (type === 'mongodb') {
      this.chatService.convertNLQToMongo(query).subscribe(
          (response: any) => {
              console.log('Response from nlq-to-mongo:', response);
              const mongoQuery = response.data.query; // Extract the MongoDB query
  
              if (!mongoQuery) {
                  console.error('MongoDB query is undefined');
                  this.chatService.addMessage({
                      sender: 'db',
                      content: 'Error: MongoDB query is undefined in response',
                  });
                  return;
              }

              this.chatService.addMessage({
                sender: 'db',
                content: `Generated Mongo query: ${mongoQuery}`
              });
  
  
              // Send MongoDB query to the backend
              this.chatService.sendMessageToMongoDB({ query: mongoQuery }).subscribe(
                (response) => {
                  console.log('MongoDB query response:', response);
                  this.chatService.addMessage({
                    sender: 'db',
                    content: JSON.stringify({ type: 'mongodb', data: response.data })
                  });
                  this.chatService.isLoadingSubject.next(false)
                },
                (error) => {
                  console.error('Error sending MongoDB query:', error);
                  this.chatService.addMessage({
                    sender: 'db',
                    content: `Error: ${error.message}`
                  });
                  this.chatService.isLoadingSubject.next(false)
                }
              );
          },
          (error: any) => {
              console.error('Error generating MongoDB query:', error);
              this.chatService.addMessage({
                  sender: 'db',
                  content: `Error generating MongoDB query: ${error.message}`
              });
          }
      );
  }
  }
  // This is for the SQL query
  handleUserMessage(message: string) {
    // Display the user's message
    this.chatService.addMessage({
      sender: 'user',
      content: message
    });
  
    // Send NLQ to backend
    this.chatService.convertNLQToSQL(message).subscribe(
      (response: any) => {
        const sqlQuery = response.sql; // Assume the backend returns { sql: "SQL QUERY" }
  
        // Display the generated SQL query
        this.chatService.addMessage({
          sender: 'db',
          content: `Generated SQL query: ${sqlQuery}`
        });
  
        // Execute the SQL query
        this.chatService.sendMessageToSQL({ query: sqlQuery });
      },
      (error) => {
        // Display error
        this.chatService.addMessage({
          sender: 'db',
          content: `Error generating SQL query: ${error.message}`
        });
      }
    );
  }
  // handleUserMessage(message: string) {
  //   // Immediately display the user's message in the chat window
  //   this.chatService.addMessage({
  //     sender: 'user',
  //     content: message
  //   });
  
  //   // Check if the input starts with 'SELECT', 'INSERT', etc., to identify SQL
  //   if (/^\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|SHOW)/i.test(message)) {
  //     console.log('Detected SQL query:', message); // Debug log
  
  //     const query = { query: message }; // Construct query object for backend
  //     console.log('Sending SQL query to backend:', query);
  //     this.chatService.sendMessageToSQL(query); // Send SQL query to backend
  //   } else {
  //     this.chatService.addMessage({
  //       sender: 'db',
  //       content: 'Error: Unrecognized command or query. Please enter a valid SQL query.'
  //     });
  //   }
  // }
}

