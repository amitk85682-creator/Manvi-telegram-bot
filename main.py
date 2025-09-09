# main.py
from telegram.ext import Application, CommandHandler
from handlers import setup_conversation_handler # handlers.py se function import kiya

def main():
    application = Application.builder().token("YOUR_TOKEN").build()
    
    # Sirf ConversationHandler ko add karein
    conv_handler = setup_conversation_handler()
    application.add_handler(conv_handler)
    
    # Aap start ya menu jaise commands add kar sakte hain
    # application.add_handler(CommandHandler("start", start_function))

    # Koi general MessageHandler nahi hai. ✅
    
    application.run_polling()

if __name__ == "__main__":
    main()
