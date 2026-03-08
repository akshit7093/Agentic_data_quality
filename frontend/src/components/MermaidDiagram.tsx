import { useEffect, useRef, useState } from 'react';
import mermaid from 'mermaid';

interface MermaidDiagramProps {
    chart: string;
}

export default function MermaidDiagram({ chart }: MermaidDiagramProps) {
    const containerRef = useRef<HTMLDivElement>(null);
    const [svgContent, setSvgContent] = useState<string>('');
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        mermaid.initialize({
            startOnLoad: false,
            theme: 'dark',
            securityLevel: 'loose',
            fontFamily: 'inherit',
        });
    }, []);

    useEffect(() => {
        let isMounted = true;

        const renderChart = async () => {
            if (!chart || !containerRef.current) return;

            try {
                setError(null);
                // Generate a unique ID to avoid DOM conflicts if multiple diagrams are rendered
                const id = `mermaid-svg-${Math.random().toString(36).substr(2, 9)}`;
                const { svg } = await mermaid.render(id, chart);

                if (isMounted) {
                    setSvgContent(svg);
                }
            } catch (err: any) {
                console.error('Mermaid rendering error:', err);
                if (isMounted) {
                    setError(err.message || 'Failed to render diagram');
                }
            }
        };

        renderChart();

        return () => {
            isMounted = false;
        };
    }, [chart]);

    if (error) {
        return (
            <div className="p-4 bg-red-900/20 border border-red-500/50 rounded text-red-400 text-sm font-mono overflow-auto">
                <p className="font-bold mb-2">Diagram rendering error:</p>
                <pre>{error}</pre>
                <div className="mt-4 text-xs text-slate-500">
                    Source:
                    <pre className="mt-1 opacity-70 whitespace-pre-wrap">{chart}</pre>
                </div>
            </div>
        );
    }

    return (
        <div
            className="w-full overflow-x-auto py-8 flex justify-center mermaid-container"
            ref={containerRef}
            dangerouslySetInnerHTML={{ __html: svgContent || `<div class="animate-pulse flex items-center gap-2 text-slate-500"><svg class="animate-spin h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path></svg> Rendering workflow diagram...</div>` }}
        />
    );
}
