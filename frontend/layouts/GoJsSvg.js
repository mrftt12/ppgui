  function getColor(color) {
    return myDiagram.themeManager.themeMap.get(myDiagram.themeManager.currentTheme).colors[color];
  }

  function init() {
    // "icons" is defined above in the previous <script>.

    // A data binding conversion function. Given an icon name, return a Geometry.
    // This assumes that all icons want to be filled.
    // This caches the Geometry, because the Geometry may be shared by multiple Shapes.
    function geoFunc(geoname) {
      var geo = icons[geoname];
      if (geo === undefined) geo = icons['heart']; // use this for an unknown icon name
      if (typeof geo === 'string') {
        geo = icons[geoname] = go.Geometry.parse(geo, true); // fill each geometry
        geo.normalize();
      }
      return geo;
    }

    // The second Diagram showcases every icon
    myDiagram = new go.Diagram('myDiagramDiv', {
      'animationManager.isEnabled': true,
      'animationManager.initialAnimationStyle': go.AnimationStyle.None,
      'undoManager.isEnabled': false,
      isReadOnly: true,
      'panningTool.isEnabled': false,
      'dragSelectingTool.isEnabled': false,
      allowZoom: false,
      'toolManager.mouseWheelBehavior': 'none',
      'toolManager.hoverDelay': 350, // changes delay for tooltips

      // pack the Nodes together, (0,0) sometimes causes clipping issues
      layout: new go.GridLayout({ spacing: new go.Size(1, 1) }),

      'toolManager.positionToolTip': (tooltip, obj) => {
        const doc_bounds = myDiagram.documentBounds;
        const toolBound = tooltip.getDocumentBounds();
        const objBound = obj.getDocumentBounds();

        const tl = new go.Point(toolBound.x, toolBound.y);
        const br = new go.Point(toolBound.right, toolBound.bottom);

        const out_left = doc_bounds.x - tl.x;
        const out_right = doc_bounds.right - br.x;

        // const out_up = doc_bounds.y - tl.y;
        const out_down = doc_bounds.bottom - br.y;

        const pos = tooltip.position;
        if (out_left > 0) {
          // tooltip.position = new go.Point(objBound.x, pos.y);
          tooltip.position = new go.Point(pos.x + out_left, pos.y);
        } else if (out_right < 0) {
          // tooltip.position = new go.Point(objBound.right - toolBound.width, pos.y);
          tooltip.position = new go.Point(pos.x + out_right, pos.y);
        }

        if (out_down < 0) {
          tooltip.position = new go.Point(
            tooltip.position.x,
            objBound.top - toolBound.height - (objBound.top - toolBound.top)
          );
        }
      }
    });

    myDiagram.themeManager.set('', {
      // a collection of colors
      colors: {
        div: '#6495ED',
        orange: '#ea2857',
        green: '#1cc1bc',
        gray: '#5b5b5b',
        white: '#F5F5F5'
      }
    });
    myDiagram.themeManager.changesDivBackground = true;

    myDiagram.nodeTemplate =
      new go.Node('Auto', {
          selectionAdorned: false,
          mouseEnter: (e, obj) => {
            // lighten the Node on hover
            if (obj.part.isSelected) return;
            let color = getColor('div');
            let br = new go.Brush(color);
            br.lightenBy(0.1);
            obj.part.elt(0).fill = br.color;
          },
          mouseLeave: (e, obj) => {
            if (obj.part.isSelected) return;
            let color = getColor('div');
            obj.part.elt(0).fill = color;
          },
          toolTip:
            new go.Adornment('Spot', { background: null })
              .add(
                new go.Placeholder({ padding: 5 }),
                new go.Panel('Auto', {
                    alignment: go.Spot.Bottom,
                    alignmentFocus: go.Spot.Top
                  })
                  .add(
                    new go.Shape('RoundedRectangle', { strokeWidth: 0 })
                      .theme('fill', 'gray'),
                    new go.TextBlock({ margin: 8, font: '16px InterVariable' })
                      .bind('text', 'geo')
                      .theme('stroke', 'white')
                  )
              )
      })
      .add(
        new go.Shape('RoundedRectangle', {
            strokeWidth: 0,
            width: 55,
            height: 55
          })
          .trigger(
            new go.AnimationTrigger('fill', { duration: 200 })  // animate the color change
          )
          .bindObject('fill', 'isSelected', (isSelected, obj) => {
            let color = getColor('div');
            let br = new go.Brush(color);
            return !isSelected ? color : br.lightenBy(0.17).color;
          }),
        new go.Shape({
            margin: 3,
            strokeWidth: 1.5,
            fill: null
          })
          .bind('geometry', 'geo', geoFunc)
          .theme('stroke', 'white')
      );

    // Convert the icons collection into an Array of JavaScript objects
    var nodeArray = [];
    for (var k in icons) {
      nodeArray.push({ geo: k, color: 'div' });
    }
    myDiagram.model.nodeDataArray = nodeArray;

    myDiagram.addDiagramListener('ObjectSingleClicked', updateSelection);

    myDiagram.select(myDiagram.nodes.first());
    updateSelection({ subject: { part: myDiagram.nodes.first() } });
  }

  function updateSelection(e) {
    let part = e.subject.part;

    // find the parent node
    while ((part.panel || part.part) != part) {
      part = part.panel || part.part;
    }

    let data = part.data;

    let geo = icons[data.geo];
    let color = getColor(data.color);

    TEMPLATE = `myDiagram.nodeTemplate =
new go.Node('Auto')
  .add(
    new go.Shape('RoundedRectangle', {
      fill: '{COLOR}',
      strokeWidth: 0,
      width: 55,
      height: 55
    }),
    new go.Shape({
      margin: 3,
      strokeWidth: 1.5,
      fill: null,
      stroke: '{COLOR2}',
      geometry: go.Geometry.parse('{GEO}', true)
    })
  );`;

    // fill out the template code with information from the selected node
    template = TEMPLATE.replaceAll('{GEO}', geo)
      .replaceAll('{COLOR}', color)
      .replaceAll('{COLOR2}', getColor('white'));

    const box = document.getElementById('codeBox');
    box.textContent = template;

    if (window.Prism) {
      // Give the code syntax highlighting
      window.Prism.highlightElement(box);
    }
  }

  window.addEventListener('DOMContentLoaded', init);