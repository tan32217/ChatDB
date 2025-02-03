import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { BehaviorSubject, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ChatService {
  private baseUrl = 'http://localhost:5004'; // Flask backend base URL
  private messagesSubject = new BehaviorSubject<{ sender: string, content: string }[]>([]);
  public isLoadingSubject = new BehaviorSubject<boolean>(false);

  messages$ = this.messagesSubject.asObservable();
  isLoading$ = this.isLoadingSubject.asObservable();

  constructor(private http: HttpClient) {}

  uploadFile(formData: FormData, type: 'sql' | 'mongodb'): Observable<any> {
    const endpoint = type === 'sql' ? '/upload/mysql' : '/upload/mongodb';
    return this.http.post(`${this.baseUrl}${endpoint}`, formData);
  }

  uploadCSV(file: File, tableName: string): Observable<any> {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('table', tableName);
    return this.http.post(`${this.baseUrl}/upload/mysql`, formData);
  }

  uploadJSON(file: File, collectionName: string): Observable<any> {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('collection', collectionName);
    return this.http.post(`${this.baseUrl}/upload/mongodb`, formData);
  }

  convertNLQToSQL(nlq: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/nlq-to-sql`, { nlq });
  }

  convertNLQToMongo(nlq: string): Observable<any> {
    return this.http.post(`${this.baseUrl}/nlq-to-mongo`, { nlq });
  }

  sendMessageToSQL(query: any): void {
    this.isLoadingSubject.next(true); // Show the loading spinner
  
    this.http.post(`${this.baseUrl}/query/mysql`, query).subscribe(
      (response) => {
        //console.log('Response from backend (SQL):', response); // Debug log
        this.addMessage({
          sender: 'db',
          content: JSON.stringify(response, null, 2) // Format response for readability
        });
        this.isLoadingSubject.next(false); // Hide the loading spinner
      },
      (error) => {
        //console.error('Error sending SQL query to backend:', error); // Debug log
        this.addMessage({
          sender: 'db',
          content: `Error: ${error.message}`
        });
        this.isLoadingSubject.next(false); // Hide the loading spinner
      }
    );
  }
  
  sendMessageToMongoDB(query: any): Observable<any> {
    this.isLoadingSubject.next(true); // Show the loading spinner
    console.log('Sending MongoDB Query:', query); // Debug log
    const queryParam = encodeURIComponent(query.query); // Encode query parameter
    // this.http.get(`${this.baseUrl}/query/mongodb?query=${queryParam}`).subscribe(
    //   (response) => {
    //     console.log('Response from backend (MongoDB):', response); // Debug log
    //     this.addMessage({
    //       sender: 'db',
    //       content: JSON.stringify(response, null, 2) // Format response for readability
    //     });
    //     this.isLoadingSubject.next(false); // Hide the loading spinner
    //   },
    //   (error) => {
    //     console.error('Error sending MongoDB query:', error); // Debug log
    //     this.addMessage({
    //       sender: 'db',
    //       content: `Error: ${error.message}`
    //     });
    //     this.isLoadingSubject.next(false); // Hide the loading spinner
    //   }
    // );
    return this.http.post(`${this.baseUrl}/query/mongodb`, query);
    //return this.http.get(`${this.baseUrl}/query/mongodb?query=${queryParam}`);
  }
  fetchMongoDBMetadata(): Observable<any> {
    console.log('Sending request to fetch MongoDB metadata...');
    return this.http.get(`${this.baseUrl}/metadata/mongodb`);
  }
  

  public addMessage(message: { sender: string, content: string }): void {
    const currentMessages = this.messagesSubject.value;
    this.messagesSubject.next([...currentMessages, message]);
  }
}

