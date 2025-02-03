import { Component, Output, EventEmitter } from '@angular/core';
import { ChatService } from '../services/chatservice';

@Component({
  selector: 'app-input-box',
  templateUrl: './input-box.component.html',
  styleUrls: ['./input-box.component.css']
})
export class InputBoxComponent {
  query: string = '';
  showAttachmentMenu = false;
  showDropdown: boolean = false;
  //@Output() sendMessage = new EventEmitter<{ query: string; type: string }>();
  //@Output() sendQuery = new EventEmitter<{ query: string; type: string }>();
  @Output() sendQuery = new EventEmitter<{ query: string; type: 'mysql' | 'mongodb' }>();


  constructor(private chatService: ChatService) {}

  toggleAttachmentMenu() {
    this.showAttachmentMenu = !this.showAttachmentMenu;
  }

  toggleDropdown() {
    this.showDropdown = !this.showDropdown;
  }



  handleFileUpload(event: Event, type: 'sql' | 'mongodb') {
    const inputElement = event.target as HTMLInputElement;
    if (!inputElement.files || inputElement.files.length === 0) return;
  
    // Close the dropdown as soon as the upload starts
    this.showDropdown = false;
  
    const file = inputElement.files[0];
    const tableOrCollectionName = prompt(
      `Enter the ${type === 'sql' ? 'table' : 'collection'} name:`
    );
  
    if (!tableOrCollectionName) {
      this.chatService.addMessage({
        sender: 'db',
        content: `Error: ${type === 'sql' ? 'Table' : 'Collection'} name is required!`
      });
      return;
    }
  
    const formData = new FormData();
    formData.append('file', file);
    formData.append(type === 'sql' ? 'table' : 'collection', tableOrCollectionName);
  
    this.chatService.uploadFile(formData, type).subscribe(
      (response) => {
        this.chatService.addMessage({
          sender: 'db',
          content: `${type === 'sql' ? 'CSV' : 'JSON'} uploaded successfully: ${response.message}`
        });
      },
      (error) => {
        this.chatService.addMessage({
          sender: 'db',
          content: `Error uploading ${type === 'sql' ? 'CSV' : 'JSON'}: ${error.error || error.message}`
        });
      }
    );
  
    inputElement.value = ''; // Reset file input
  }
  submitQuery(type: 'mysql' | 'mongodb') {
    if (this.query.trim()) {
      this.sendQuery.emit({ query: this.query, type }); // Use explicit type
      this.query = ''; // Reset query after emitting
    }
  }

  // submitQuery() {
  //   if (this.query.trim()) {
  //     this.sendMessage.emit(this.query);
  //     this.query = '';
  //   }
  // }
}
