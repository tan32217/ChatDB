import { Component, Input } from '@angular/core';

@Component({
  selector: 'app-chat-message',
  templateUrl: './chat-message.component.html',
  styleUrls: ['./chat-message.component.css']
})
export class ChatMessageComponent {
  @Input() message: any;

  getParsedContent(): any[] {
    let parsedContent;
    try {
      // Parse the message content if it is a string
      parsedContent = typeof this.message.content === 'string'
        ? JSON.parse(this.message.content)
        : this.message.content;
    } catch (error) {
      console.error('Failed to parse JSON content:', error);
      return [];
    }
  
    // Ensure the content is always an array
    if (Array.isArray(parsedContent)) {
      return parsedContent;
    }
    
    // If it's a single object, wrap it in an array for consistency
    if (typeof parsedContent === 'object' && parsedContent !== null) {
      return [parsedContent];
    }
  
    return [];
  }

  isMongoData(): boolean {
    const parsedContent = this.getParsedContent();
    return Array.isArray(parsedContent) &&
           parsedContent.every((item: any) => typeof item === 'object' && '_id' in item);
  }

  getObjectKeys(doc: any): string[] {
    return Object.keys(doc);
  }

  isTableData(): boolean {
    let parsedContent;
    try {
      // Parse the stringified JSON if it's not already parsed
      parsedContent = typeof this.message.content === 'string'
        ? JSON.parse(this.message.content)
        : this.message.content;
    } catch (error) {
      console.error('Failed to parse JSON content:', error);
      return false;
    }
  
    // Check if parsed content is an array of objects
    return Array.isArray(parsedContent) && 
           parsedContent.every((item: any) => typeof item === 'object');
  }
  
  getTableHeaders(): string[] {
    let parsedContent;
    try {
      // Parse the stringified JSON if it's not already parsed
      parsedContent = typeof this.message.content === 'string'
        ? JSON.parse(this.message.content)
        : this.message.content;
      
    } catch (error) {
      console.error('Failed to parse JSON content:', error);
      return [];
    }
    // console.log('Parsed content:', parsedContent);
    if (Array.isArray(parsedContent) && parsedContent.length > 0) {
      return Object.keys(parsedContent[0]);
    }
    return [];
  }
}

// import { Component, Input } from '@angular/core';

// @Component({
//   selector: 'app-chat-message',
//   templateUrl: './chat-message.component.html',
//   styleUrls: ['./chat-message.component.css']
// })
// export class ChatMessageComponent {
//   @Input() message!: { sender: string, content: string };
// }
