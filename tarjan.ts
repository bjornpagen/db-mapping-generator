export function tarjan(graph: { [key: string]: string[] }): string[][] {
	const indexMap: { [key: string]: number } = {}
	const lowLinkMap: { [key: string]: number } = {}
	const stack: string[] = []
	const onStack: { [key: string]: boolean } = {}
	const sccs: string[][] = []
	let index = 0

	function strongConnect(vertex: string) {
		indexMap[vertex] = index
		lowLinkMap[vertex] = index
		index++
		stack.push(vertex)
		onStack[vertex] = true

		const neighbors = graph[vertex] || []
		for (const neighbor of neighbors) {
			if (!(neighbor in indexMap)) {
				strongConnect(neighbor)
				lowLinkMap[vertex] = Math.min(lowLinkMap[vertex], lowLinkMap[neighbor])
			} else if (onStack[neighbor]) {
				lowLinkMap[vertex] = Math.min(lowLinkMap[vertex], indexMap[neighbor])
			}
		}

		if (lowLinkMap[vertex] === indexMap[vertex]) {
			const scc: string[] = []
			let w: string
			do {
				w = stack.pop() as string
				onStack[w] = false
				scc.push(w)
			} while (w !== vertex)
			sccs.push(scc)
		}
	}

	for (const vertex of Object.keys(graph)) {
		if (!(vertex in indexMap)) {
			strongConnect(vertex)
		}
	}

	return sccs
}
