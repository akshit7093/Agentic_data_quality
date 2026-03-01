import { Brain, Terminal, Database, FileText, CheckCircle, Activity } from 'lucide-react';

interface Message {
    role: string;
    content: string;
}

interface ExecutionTraceViewerProps {
    messages: Message[];
}

interface ParsedBlock {
    type: 'context' | 'thought' | 'action' | 'output';
    content: string;
    actionPayload?: string;
    report?: string;
}

const parseContent = (content: string, role: string): ParsedBlock[] => {
    const blocks: ParsedBlock[] = [];

    if (role === 'system') {
        blocks.push({ type: 'context', content });
        return blocks;
    }

    // Handle QUERY RESULTS explicitly
    if (content.startsWith('QUERY RESULTS:')) {
        blocks.push({ type: 'output', content: content.replace('QUERY RESULTS:', '').trim() });
        return blocks;
    }

    // Extract JSON blocks (Actions)
    const jsonRegex = /```json\s*(\{[\s\S]*?\})\s*```/g;
    let lastIndex = 0;
    let match;

    while ((match = jsonRegex.exec(content)) !== null) {
        const thoughtText = content.substring(lastIndex, match.index).trim();
        if (thoughtText) {
            blocks.push({ type: 'thought', content: thoughtText });
        }
        blocks.push({ type: 'action', content: 'Tool Call', actionPayload: match[1] });
        lastIndex = jsonRegex.lastIndex;
    }

    const remaining = content.substring(lastIndex).trim();
    if (remaining) {
        // Check if remaining contains <REPORT> or <METADATA> case-insensitively
        const reportMatch = /<(REPORT|METADATA)>([\s\S]*?)<\/\1>/i.exec(remaining);
        if (reportMatch) {
            const thoughtText = remaining.substring(0, reportMatch.index).trim();
            if (thoughtText) {
                blocks.push({ type: 'thought', content: thoughtText });
            }
            blocks.push({ type: 'output', content: 'Final Report Generated', report: reportMatch[2].trim() });

            const afterReport = remaining.substring(reportMatch.index + reportMatch[0].length).trim();
            if (afterReport) {
                blocks.push({ type: 'thought', content: afterReport });
            }
        } else {
            blocks.push({ type: 'thought', content: remaining });
        }
    }

    return blocks;
};

export default function ExecutionTraceViewer({ messages }: ExecutionTraceViewerProps) {
    if (!messages || messages.length === 0) {
        return (
            <div className="text-gray-500 text-center py-10 border rounded-xl bg-gray-50">
                <Activity className="w-8 h-8 mx-auto mb-3 text-gray-400" />
                <p>No execution trace recorded.</p>
            </div>
        );
    }

    return (
        <div className="space-y-6">
            <div className="relative border-l-2 border-gray-200 ml-4 space-y-8 pb-4">
                {messages.map((msg, msgIdx) => {
                    const blocks = parseContent(msg.content, msg.role);
                    return blocks.map((block, blockIdx) => {

                        let Icon = Terminal;
                        let iconColor = 'text-gray-400';
                        let bgColor = 'bg-white';
                        let borderColor = 'border-gray-200';
                        let title = '';

                        if (block.type === 'context') {
                            Icon = Database;
                            iconColor = 'text-blue-500';
                            borderColor = 'border-blue-200';
                            bgColor = 'bg-blue-50';
                            title = 'System Context';
                        } else if (block.type === 'thought') {
                            Icon = Brain;
                            iconColor = 'text-purple-500';
                            borderColor = 'border-purple-200';
                            bgColor = 'bg-white';
                            title = 'Agent Reasoning';
                        } else if (block.type === 'action') {
                            Icon = Terminal;
                            iconColor = 'text-amber-500';
                            borderColor = 'border-amber-200';
                            bgColor = 'bg-slate-900';
                            title = 'Tool Execution';
                        } else if (block.type === 'output') {
                            Icon = FileText;
                            iconColor = 'text-emerald-500';
                            borderColor = 'border-emerald-200';
                            bgColor = 'bg-emerald-50';
                            title = block.report ? 'Agent Report' : 'Tool Result';
                        }

                        return (
                            <div key={`${msgIdx}-${blockIdx}`} className="relative pl-8">
                                <span className="absolute -left-3.5 top-2 bg-white p-1 rounded-full border border-gray-200">
                                    <Icon className={`w-4 h-4 ${iconColor}`} />
                                </span>

                                <div className={`rounded-lg border ${borderColor} ${bgColor} overflow-hidden shadow-sm`}>
                                    <div className={`px-4 py-2 text-xs font-semibold uppercase tracking-wider ${block.type === 'action' ? 'text-gray-400 border-b border-slate-700' : 'text-gray-500 border-b border-gray-100 flex items-center'}`}>
                                        {title}
                                    </div>
                                    <div className="p-4 text-sm">
                                        {block.type === 'action' && block.actionPayload ? (
                                            <pre className="text-emerald-400 font-mono text-xs whitespace-pre-wrap overflow-x-auto">
                                                {block.actionPayload}
                                            </pre>
                                        ) : block.type === 'output' && block.report ? (
                                            <div className="prose prose-sm prose-emerald max-w-none text-gray-800 whitespace-pre-wrap">
                                                {block.report}
                                            </div>
                                        ) : block.type === 'output' ? (
                                            <pre className="text-gray-700 font-mono text-xs whitespace-pre-wrap overflow-x-auto">
                                                {block.content}
                                            </pre>
                                        ) : (
                                            <div className="text-gray-700 leading-relaxed whitespace-pre-wrap">
                                                {block.content}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        );
                    });
                })}

                {/* End of trace marker */}
                <div className="relative pl-8">
                    <span className="absolute -left-3.5 top-0 bg-white p-1 rounded-full border border-emerald-200">
                        <CheckCircle className="w-4 h-4 text-emerald-500" />
                    </span>
                    <div className="text-sm font-medium text-gray-500 pt-1">Trace Complete</div>
                </div>
            </div>
        </div>
    );
}
