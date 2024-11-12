const searchInput = document.getElementById('searchInput');
const suggestionsBox = document.getElementById('suggestions');

// Function to hide suggestions
const hideSuggestions = () => {
  suggestionsBox.style.display = 'none';
};

// Function to show suggestions if there's a search term
const showSuggestionsIfNeeded = async () => {
  const query = searchInput.value.trim();
  if (query.length > 0) {
    await fetchSuggestions();
  }
};

// Handle focus on input
searchInput.addEventListener('focus', showSuggestionsIfNeeded);

// Handle clicks outside suggestions
document.addEventListener('click', (event) => {
  const isClickInside = suggestionsBox.contains(event.target) || searchInput.contains(event.target);
  if (!isClickInside) {
    hideSuggestions();
  }
});

// Handle escape key
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    hideSuggestions();
    searchInput.blur(); // Optional: also remove focus from input
  }
});

// Debounce function to limit the rate of API calls
function debounce(func, wait) {
  let timeout;
  return function () {
    const args = arguments;
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(null, args), wait);
  };
}

// Fetch suggestions with debounce
const fetchSuggestions = debounce(async () => {
  const query = searchInput.value;

  if (query.length < 1) {
    suggestionsBox.style.display = 'none';
    return;
  }

  try {
    const response = await fetch(`/api/suggestions?query=${encodeURIComponent(query)}`);
    const data = await response.json();

    if (data.suggestions && data.suggestions.length > 0) {
      suggestionsBox.innerHTML = '';
      data.suggestions.forEach(item => {
        const suggestion = document.createElement('div');
        suggestion.className = 'suggestion-item';
        suggestion.innerHTML = `
          <strong>${item.title}</strong> (${item.release_date || 'N/A'})<br>
          <small>${item.overview || 'No overview available'}</small>
        `;
        suggestion.addEventListener('click', () => {
          searchInput.value = item.title;
          suggestionsBox.style.display = 'none';
        });
        suggestionsBox.appendChild(suggestion);
      });
      suggestionsBox.style.display = 'block';
    } else {
      suggestionsBox.style.display = 'none';
    }
  } catch (error) {
    console.error('Error fetching suggestions:', error);
    suggestionsBox.style.display = 'none';
  }
}, 300);

searchInput.addEventListener('input', fetchSuggestions);

document.getElementById('searchForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const start_name = document.getElementById('searchInput').value;
  // const path_size = document.getElementById('steps').value;

  const response = await fetch(`/api/movies?start_name=${start_name}`);
  const data = await response.json();

  renderMovieConnectionsGraphWithTooltip(data);
});


function renderMovieConnectionsGraphWithTooltip(data) {
  const container = document.getElementById('movie-container');
  container.innerHTML = '';

  // Set dimensions for the graph
  const width = 800;
  const height = 600;

  // Create an SVG element using D3
  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const nodesMap = new Map();
  const links = [];

  // Traverse through the data to create nodes and links
  data.forEach((entry) => {
    let previousNodeId = null;

    entry.path.forEach((element, index) => {
      if (typeof element === 'object' && element.title) {
        // Movie node
        const movieId = `movie-${element.id}`;

        if (!nodesMap.has(movieId)) {
          nodesMap.set(movieId, {
            id: movieId,
            label: element.title,
            type: 'movie',
            overview: element.overview || 'No overview available',
            release_date: element.release_date || 'N/A'
          });
        }

        if (previousNodeId) {
          links.push({
            source: previousNodeId,
            target: movieId
          });
        }

        previousNodeId = movieId;
      } else if (typeof element === 'object' && element.name) {
        // Actor node
        const actorId = `actor-${element.name.replace(/\s+/g, '-')}`;

        if (!nodesMap.has(actorId)) {
          nodesMap.set(actorId, {
            id: actorId,
            label: element.name,
            type: 'actor'
          });
        }

        if (previousNodeId) {
          links.push({
            source: previousNodeId,
            target: actorId
          });
        }

        previousNodeId = actorId;
      }
    });
  });

  const nodes = Array.from(nodesMap.values());

  // Create a simulation for positioning nodes
  const simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(150))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(width / 2, height / 2));

  // Draw links (edges)
  const link = svg.append('g')
    .attr('class', 'links')
    .selectAll('line')
    .data(links)
    .enter().append('line')
    .attr('stroke', '#999')
    .attr('stroke-width', 2)
    .attr('stroke-opacity', 0.6);

  // Draw nodes
  const node = svg.append('g')
    .attr('class', 'nodes')
    .selectAll('circle')
    .data(nodes)
    .enter().append('circle')
    .attr('r', 15)
    .attr('fill', d => d.type === 'movie' ? '#1f77b4' : '#2ca02c')
    .attr('stroke', '#333')
    .attr('stroke-width', 1.5)
    .on('mouseover', handleMouseOver)
    .on('mouseout', handleMouseOut)
    .call(d3.drag()
      .on('start', dragStarted)
      .on('drag', dragged)
      .on('end', dragEnded));

  // Add labels to nodes
  const labels = svg.append('g')
    .attr('class', 'labels')
    .selectAll('text')
    .data(nodes)
    .enter().append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', -20)
    .attr('font-size', '12px')
    .text(d => d.label);

  // Tooltip for additional information on nodes
  const tooltip = d3.select('body').append('div')
    .attr('class', 'tooltip')
    .style('position', 'absolute')
    .style('padding', '8px')
    .style('background', 'rgba(0,0,0,0.7)')
    .style('color', '#fff')
    .style('border-radius', '4px')
    .style('visibility', 'hidden');

  // Update the positions of nodes and links during the simulation
  simulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);

    node
      .attr('cx', d => d.x)
      .attr('cy', d => d.y);

    labels
      .attr('x', d => d.x)
      .attr('y', d => d.y);
  });

  // Functions for dragging nodes
  function dragStarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }

  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }

  function dragEnded(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }

  // Mouseover event handler to show tooltip
  function handleMouseOver(event, d) {
    tooltip
      .style('visibility', 'visible')
      .html(`<strong>${d.label}</strong><br>${d.type === 'movie' ? d.overview : ''}<br>${d.release_date ? `Release Date: ${d.release_date}` : ''}`);
    
    d3.select(this)
      .transition()
      .duration(200)
      .attr('r', 20)
      .attr('stroke-width', 3);
  }

  // Mouseout event handler to hide tooltip
  function handleMouseOut(event, d) {
    tooltip.style('visibility', 'hidden');
    
    d3.select(this)
      .transition()
      .duration(200)
      .attr('r', 15)
      .attr('stroke-width', 1.5);
  }

  // Move the tooltip with the mouse
  svg.on('mousemove', (event) => {
    tooltip.style('top', (event.pageY + 10) + 'px').style('left', (event.pageX + 10) + 'px');
  });
}


function renderMovieConnectionsGraph(data) {
  const container = document.getElementById('movie-container');
  container.innerHTML = '';

  // Set dimensions for the graph
  const width = 800;
  const height = 600;

  // Create an SVG element using D3
  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const nodesMap = new Map();
  const links = [];

  // Traverse through the data to create nodes and links
  data.forEach((entry) => {
    let previousNodeId = null;

    entry.path.forEach((element, index) => {
      if (typeof element === 'object' && element.title) {
        // Movie node
        const movieId = `movie-${element.id}`;

        if (!nodesMap.has(movieId)) {
          nodesMap.set(movieId, {
            id: movieId,
            label: element.title,
            type: 'movie',
            overview: element.overview || 'No overview available',
            release_date: element.release_date || 'N/A'
          });
        }

        if (previousNodeId) {
          links.push({
            source: previousNodeId,
            target: movieId
          });
        }

        previousNodeId = movieId;
      } else if (typeof element === 'object' && element.name) {
        // Actor node
        const actorId = `actor-${element.name.replace(/\s+/g, '-')}`;

        if (!nodesMap.has(actorId)) {
          nodesMap.set(actorId, {
            id: actorId,
            label: element.name,
            type: 'actor'
          });
        }

        if (previousNodeId) {
          links.push({
            source: previousNodeId,
            target: actorId
          });
        }

        previousNodeId = actorId;
      }
    });
  });

  const nodes = Array.from(nodesMap.values());

  // Create a simulation for positioning nodes
  const simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(150))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(width / 2, height / 2));

  // Draw links (edges)
  const link = svg.append('g')
    .attr('class', 'links')
    .selectAll('line')
    .data(links)
    .enter().append('line')
    .attr('stroke', '#999')
    .attr('stroke-width', 2)
    .attr('stroke-opacity', 0.6);

  // Draw nodes
  const node = svg.append('g')
    .attr('class', 'nodes')
    .selectAll('circle')
    .data(nodes)
    .enter().append('circle')
    .attr('r', 15)
    .attr('fill', d => d.type === 'movie' ? '#1f77b4' : '#2ca02c')
    .attr('stroke', '#333')
    .attr('stroke-width', 1.5)
    .on('mouseover', handleMouseOver)
    .on('mouseout', handleMouseOut)
    .call(d3.drag()
      .on('start', dragStarted)
      .on('drag', dragged)
      .on('end', dragEnded));

  // Add labels to nodes
  const labels = svg.append('g')
    .attr('class', 'labels')
    .selectAll('text')
    .data(nodes)
    .enter().append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', -20)
    .attr('font-size', '12px')
    .text(d => d.label);

  // Tooltip for additional information on nodes
  const tooltip = d3.select('body').append('div')
    .attr('class', 'tooltip')
    .style('position', 'absolute')
    .style('padding', '8px')
    .style('background', 'rgba(0,0,0,0.7)')
    .style('color', '#fff')
    .style('border-radius', '4px')
    .style('visibility', 'hidden');

  // Update the positions of nodes and links during the simulation
  simulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);

    node
      .attr('cx', d => d.x)
      .attr('cy', d => d.y);

    labels
      .attr('x', d => d.x)
      .attr('y', d => d.y);
  });

  // Functions for dragging nodes
  function dragStarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }

  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }

  function dragEnded(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }

  // Mouseover event handler to show tooltip
  function handleMouseOver(event, d) {
    tooltip
      .style('visibility', 'visible')
      .html(`<strong>${d.label}</strong><br>${d.type === 'movie' ? d.overview : ''}<br>${d.release_date ? `Release Date: ${d.release_date}` : ''}`);
    
    d3.select(this)
      .transition()
      .duration(200)
      .attr('r', 20)
      .attr('stroke-width', 3);
  }

  // Mouseout event handler to hide tooltip
  function handleMouseOut(event, d) {
    tooltip.style('visibility', 'hidden');
    
    d3.select(this)
      .transition()
      .duration(200)
      .attr('r', 15)
      .attr('stroke-width', 1.5);
  }

  // Move the tooltip with the mouse
  svg.on('mousemove', (event) => {
    tooltip.style('top', (event.pageY + 10) + 'px').style('left', (event.pageX + 10) + 'px');
  });
}

function renderMovieConnectionsGraph(data) {
  const container = document.getElementById('movie-container');
  container.innerHTML = '';

  // Set dimensions for the graph
  const width = 800;
  const height = 600;

  // Create an SVG element using D3
  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const nodesMap = new Map();
  const links = [];

  // Traverse through the data to create nodes and links
  data.forEach((entry) => {
    let previousNodeId = null;

    entry.path.forEach((element, index) => {
      if (typeof element === 'object' && element.title) {
        // Movie node
        const movieId = `movie-${element.id}`;

        if (!nodesMap.has(movieId)) {
          nodesMap.set(movieId, {
            id: movieId,
            label: element.title,
            type: 'movie'
          });
        }

        if (previousNodeId) {
          links.push({
            source: previousNodeId,
            target: movieId
          });
        }

        previousNodeId = movieId;
      } else if (typeof element === 'object' && element.name) {
        // Actor node
        const actorId = `actor-${element.name.replace(/\s+/g, '-')}`;

        if (!nodesMap.has(actorId)) {
          nodesMap.set(actorId, {
            id: actorId,
            label: element.name,
            type: 'actor'
          });
        }

        if (previousNodeId) {
          links.push({
            source: previousNodeId,
            target: actorId
          });
        }

        previousNodeId = actorId;
      }
    });
  });

  const nodes = Array.from(nodesMap.values());

  // Create a simulation for positioning nodes
  const simulation = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(150))
    .force('charge', d3.forceManyBody().strength(-300))
    .force('center', d3.forceCenter(width / 2, height / 2));

  // Draw links (edges)
  const link = svg.append('g')
    .attr('class', 'links')
    .selectAll('line')
    .data(links)
    .enter().append('line')
    .attr('stroke', '#999')
    .attr('stroke-width', 2);

  // Draw nodes
  const node = svg.append('g')
    .attr('class', 'nodes')
    .selectAll('circle')
    .data(nodes)
    .enter().append('circle')
    .attr('r', 10)
    .attr('fill', d => d.type === 'movie' ? 'blue' : 'green')
    .call(d3.drag()
      .on('start', dragStarted)
      .on('drag', dragged)
      .on('end', dragEnded));

  // Add labels to nodes
  const labels = svg.append('g')
    .attr('class', 'labels')
    .selectAll('text')
    .data(nodes)
    .enter().append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', -15)
    .text(d => d.label);

  // Update the positions of nodes and links during the simulation
  simulation.on('tick', () => {
    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);

    node
      .attr('cx', d => d.x)
      .attr('cy', d => d.y);

    labels
      .attr('x', d => d.x)
      .attr('y', d => d.y);
  });

  // Functions for dragging nodes
  function dragStarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }

  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }

  function dragEnded(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }
} 
