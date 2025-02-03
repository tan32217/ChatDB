import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';

import { AppComponent } from './app.component';
import { ChatWindowComponent } from './chat-window/chat-window.component';
import { ChatMessageComponent } from './chat-message/chat-message.component';
import { InputBoxComponent } from './input-box/input-box.component';
import { LoadingSpinnerComponent } from './loading-spinner/loading-spinner.component';
import { HttpClientModule } from '@angular/common/http';
import { ChatService } from './services/chatservice';

@NgModule({
  declarations: [
    AppComponent,
    ChatWindowComponent,
    ChatMessageComponent,
    InputBoxComponent,
    LoadingSpinnerComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,  // Required for ngModel in InputBoxComponent
    HttpClientModule  
  ],
  providers: [ChatService],
  bootstrap: [AppComponent]
})
export class AppModule { }
