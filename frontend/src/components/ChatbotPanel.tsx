import { useState, useRef, useEffect } from 'react';
import { Send, Bot, User, Loader2 } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface Message {
    role: 'user' | 'assistant' | 'system';
    content: string;
}

interface ChatbotPanelProps {
    validationId: string;
    initialHistory?: Message[];
}

export default function ChatbotPanel({ validationId, initialHistory = [] }: ChatbotPanelProps) {
    const [messages, setMessages] = useState<Message[]>(initialHistory);
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const messagesEndRef = useRef<HTMLDivElement>(null);

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    const handleSend = async () => {
        if (!input.trim() || isLoading) return;

        const userMsg: Message = { role: 'user', content: input };
        setMessages(prev => [...prev, userMsg]);
        setInput('');
        setIsLoading(true);

        try {
            const response = await fetch('/api/v1/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message: input,
                    validation_id: validationId,
                    history: messages.concat(userMsg)
                }),
            });

            if (!response.ok) {
                throw new Error('Failed to get response from AI');
            }

            const data = await response.json();
            setMessages(data.history);
        } catch (error) {
            console.error('Chat error:', error);
            setMessages(prev => [...prev, {
                role: 'system',
                content: 'Error: Could not connect to the AI assistant. Please try again later.'
            }]);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="flex flex-col h-[600px] bg-royal-green-900/50 border border-royal-green-600 rounded-xl overflow-hidden">
            {/* Header */}
            <div className="px-6 py-4 border-b border-royal-green-600 bg-royal-green-900 flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <div className="bg-primary/20 p-1.5 rounded-lg border border-primary/30">
                        <Bot className="w-5 h-5 text-primary" />
                    </div>
                    <div>
                        <h3 className="text-sm font-bold text-slate-100 uppercase tracking-widest">Hybrid Chatbot Agent</h3>
                        <p className="text-[10px] text-slate-400 font-black uppercase tracking-widest">Connected to Metadata RAG</p>
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
                    <span className="text-[10px] font-black text-emerald-400 uppercase tracking-widest">Active</span>
                </div>
            </div>

            {/* Messages Area */}
            <div className="flex-1 overflow-y-auto p-6 space-y-6">
                {messages.filter(m => m.role !== 'system').map((msg, idx) => (
                    <div key={idx} className={`flex gap-4 ${msg.role === 'user' ? 'flex-row-reverse' : ''}`}>
                        <div className={`w-8 h-8 rounded-lg flex items-center justify-center shrink-0 border 
              ${msg.role === 'user'
                                ? 'bg-royal-green-800 border-royal-green-600 text-slate-300'
                                : 'bg-primary/10 border-primary/20 text-primary'}`}>
                            {msg.role === 'user' ? <User className="w-4 h-4" /> : <Bot className="w-4 h-4" />}
                        </div>

                        <div className={`max-w-[85%] rounded-2xl p-4 shadow-xl border
              ${msg.role === 'user'
                                ? 'bg-royal-green-800/80 border-royal-green-600 text-slate-200 rounded-tr-none'
                                : 'bg-royal-green-900 border-royal-green-700 text-slate-300 rounded-tl-none'}`}>

                            <div className="prose prose-invert prose-sm max-w-none 
                prose-headings:text-slate-100 prose-headings:font-bold prose-headings:uppercase prose-headings:tracking-widest
                prose-p:leading-relaxed prose-strong:text-primary
                prose-table:border prose-table:border-royal-green-700
                prose-th:bg-royal-green-950 prose-th:px-3 prose-th:py-2 prose-th:text-[10px] prose-th:font-black prose-th:uppercase prose-th:tracking-widest prose-th:text-slate-200
                prose-td:px-3 prose-td:py-2 prose-td:border-t prose-td:border-royal-green-700 prose-td:text-slate-400">
                                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                                    {msg.content}
                                </ReactMarkdown>
                            </div>
                        </div>
                    </div>
                ))}

                {isLoading && (
                    <div className="flex gap-4 animate-in fade-in slide-in-from-bottom-2">
                        <div className="w-8 h-8 rounded-lg bg-primary/10 border border-primary/20 text-primary flex items-center justify-center animate-pulse">
                            <Bot className="w-4 h-4" />
                        </div>
                        <div className="bg-royal-green-900 border border-royal-green-700 rounded-2xl rounded-tl-none p-4 shadow-xl">
                            <Loader2 className="w-5 h-5 text-primary animate-spin" />
                        </div>
                    </div>
                )}
                <div ref={messagesEndRef} />
            </div>

            {/* Input Area */}
            <div className="p-4 border-t border-royal-green-600 bg-royal-green-950/50">
                <div className="relative group">
                    <input
                        type="text"
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyDown={(e) => e.key === 'Enter' && handleSend()}
                        placeholder="Ask the AI agent about your data..."
                        className="w-full bg-royal-green-900 border border-royal-green-700 rounded-xl py-3 pl-4 pr-12 text-slate-200 placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-primary focus:border-primary transition-all"
                    />
                    <button
                        onClick={handleSend}
                        disabled={!input.trim() || isLoading}
                        className="absolute right-2 top-1/2 -translate-y-1/2 p-2 text-primary hover:bg-primary/10 rounded-lg transition-colors disabled:opacity-50"
                    >
                        {isLoading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Send className="w-5 h-5" />}
                    </button>
                </div>
                <p className="text-[8px] text-slate-500 mt-2 font-black uppercase tracking-widest text-center">
                    Agent can execute SQL, filter, pivot and generate charts based on your request.
                </p>
            </div>
        </div>
    );
}
