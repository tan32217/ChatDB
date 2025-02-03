import { Component, ElementRef, OnInit,  AfterViewChecked, ViewChild } from '@angular/core';
import { ChatService } from '../services/chatservice';

@Component({
  selector: 'app-chat-window',
  templateUrl: './chat-window.component.html',
  styleUrls: ['./chat-window.component.css']
})
export class ChatWindowComponent implements OnInit, AfterViewChecked  {
  @ViewChild('chatContainer') chatContainer!: ElementRef;
  messages$ = this.chatService.messages$;
  isLoading$ = this.chatService.isLoading$;

  constructor(private chatService: ChatService) {}

  ngOnInit(): void {}
  ngAfterViewChecked(): void {
    this.scrollToBottom();
  }

  private scrollToBottom(): void {
    try {
      this.chatContainer.nativeElement.scrollTop = this.chatContainer.nativeElement.scrollHeight;
    } catch (err) {
      console.error('Scroll failed:', err);
    }
  }

  fetchMetadata() {
    this.chatService.fetchMongoDBMetadata().subscribe((metadata) => {
      this.chatService.addMessage({
        sender: 'db',
        content: JSON.stringify(metadata)
      });
    });
  }
}


// import { Component, OnInit } from '@angular/core';
// import { ChatService } from '../services/chatservice';

// @Component({
//   selector: 'app-chat-window',
//   templateUrl: './chat-window.component.html',
//   styleUrls: ['./chat-window.component.css']
// })
// export class ChatWindowComponent implements OnInit {
//   messages = this.chatService.messages;
//   isLoading = this.chatService.isLoading;

//   constructor(private chatService: ChatService) {}

//   ngOnInit(): void {}
// }
